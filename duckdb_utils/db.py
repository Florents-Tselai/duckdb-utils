import os
import pathlib
import secrets
from typing import (
    Callable,
    Union,
    Optional,
    List,
    Dict, Any,
    Iterable, Generator, Tuple,
cast
)
import contextlib
import duckdb
import sqlite_utils
from sqlite_fts4 import rank_bm25
from sqlite_utils.db import (Table, ForeignKeysType, resolve_extracts, validate_column_names, COLUMN_TYPE_MAPPING,
                             Queryable,
                             jsonify_if_needed, ForeignKey, AlterError, Queryable, View, Column,
                             DEFAULT, fix_square_braces, PrimaryKeyRequired, Default, SQLITE_MAX_VARS, Trigger,
                             iterdump)
from sqlite_utils.utils import (
    chunks,
    hash_record,
    sqlite3,
    OperationalError,
    suggest_column_types,
    types_for_column_types,
    column_affinity,
    progressbar,
    find_spatialite,
_flatten
)

import itertools

######### Monkey-patching #########
_COUNTS_TABLE_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS "{}"(
   "table" TEXT PRIMARY KEY,
   count INTEGER DEFAULT 0
);
""".strip()



class Database:
    """
    Wrapper for a SQLite database connection that adds a variety of useful utility methods.

    To create an instance::

        # create data.db file, or open existing:
        db = Database("data.db")
        # Create an in-memory database:
        dB = Database(memory=True)

    :param filename_or_conn: String path to a file, or a ``pathlib.Path`` object, or a
      ``sqlite3`` connection
    :param memory: set to ``True`` to create an in-memory database
    :param memory_name: creates a named in-memory database that can be shared across multiple connections
    :param recreate: set to ``True`` to delete and recreate a file database (**dangerous**)
    :param recursive_triggers: defaults to ``True``, which sets ``PRAGMA recursive_triggers=on;`` -
      set to ``False`` to avoid setting this pragma
    :param tracer: set a tracer function (``print`` works for this) which will be called with
      ``sql, parameters`` every time a SQL query is executed
    :param use_counts_table: set to ``True`` to use a cached counts table, if available. See
      :ref:`python_api_cached_table_counts`
    :param strict: Apply STRICT mode to all created tables (unless overridden)
    """

    _counts_table_name = "_counts"
    use_counts_table = False

    def __init__(
            self,
            filename_or_conn: Optional[Union[str, pathlib.Path, duckdb.DuckDBPyConnection]] = None,
            memory: bool = False,
            memory_name: Optional[str] = None,
            recreate: bool = False,
            recursive_triggers: bool = True,
            tracer: Optional[Callable] = None,
            use_counts_table: bool = False,
            execute_plugins: bool = True,
            strict: bool = False,
    ):
        assert (filename_or_conn is not None and (not memory and not memory_name)) or (
                filename_or_conn is None and (memory or memory_name)
        ), "Either specify a filename_or_conn or pass memory=True"
        if memory_name:
            uri = "file:{}?mode=memory&cache=shared".format(memory_name)
            self.conn = duckdb.connect(
                uri,
                uri=True,
                check_same_thread=False,
            )
        elif memory or filename_or_conn == ":memory:":
            self.conn = duckdb.connect(":memory:")
        elif isinstance(filename_or_conn, (str, pathlib.Path)):
            if recreate and os.path.exists(filename_or_conn):
                try:
                    os.remove(filename_or_conn)
                except OSError:
                    # Avoid mypy and __repr__ errors, see:
                    # https://github.com/simonw/sqlite-utils/issues/503
                    self.conn = duckdb.connect(":memory:")
                    raise
            self.conn = duckdb.connect(str(filename_or_conn))
        else:
            assert not recreate, "recreate cannot be used with connections, only paths"
            self.conn = filename_or_conn
        self._tracer = tracer
        # if recursive_triggers:
        # self.execute("PRAGMA recursive_triggers=on;")
        self._registered_functions: set = set()
        self.use_counts_table = use_counts_table
        if execute_plugins:
            pass
            # pm.hook.prepare_connection(conn=self.conn)
        self.strict = strict

    def close(self):
        "Close the SQLite connection, and the underlying database file"
        self.conn.close()

    @contextlib.contextmanager
    def ensure_autocommit_off(self):
        """
        Ensure autocommit is off for this database connection.

        Example usage::

            with db.ensure_autocommit_off():
                # do stuff here

        This will reset to the previous autocommit state at the end of the block.
        """
        old_isolation_level = self.conn.isolation_level
        try:
            self.conn.isolation_level = None
            yield
        finally:
            self.conn.isolation_level = old_isolation_level

    @contextlib.contextmanager
    def tracer(self, tracer: Optional[Callable] = None):
        """
        Context manager to temporarily set a tracer function - all executed SQL queries will
        be passed to this.

        The tracer function should accept two arguments: ``sql`` and ``parameters``

        Example usage::

            with db.tracer(print):
                db["creatures"].insert({"name": "Cleo"})

        See :ref:`python_api_tracing`.

        :param tracer: Callable accepting ``sql`` and ``parameters`` arguments
        """
        prev_tracer = self._tracer
        self._tracer = tracer or print
        try:
            yield self
        finally:
            self._tracer = prev_tracer

    def __getitem__(self, table_name: str) -> Union["Table", "View"]:
        """
        ``db[table_name]`` returns a :class:`.Table` object for the table with the specified name.
        If the table does not exist yet it will be created the first time data is inserted into it.

        :param table_name: The name of the table
        """
        return self.table(table_name)

    def __repr__(self) -> str:
        return "<Database {}>".format(self.conn)

    def register_function(
            self,
            fn: Optional[Callable] = None,
            deterministic: bool = False,
            replace: bool = False,
            name: Optional[str] = None,
    ):
        """
        ``fn`` will be made available as a function within SQL, with the same name and number
        of arguments. Can be used as a decorator::

            @db.register_function
            def upper(value):
                return str(value).upper()

        The decorator can take arguments::

            @db.register_function(deterministic=True, replace=True)
            def upper(value):
                return str(value).upper()

        See :ref:`python_api_register_function`.

        :param fn: Function to register
        :param deterministic: set ``True`` for functions that always returns the same output for a given input
        :param replace: set ``True`` to replace an existing function with the same name - otherwise throw an error
        :param name: name of the SQLite function - if not specified, the Python function name will be used
        """

        def register(fn):
            fn_name = name or fn.__name__
            arity = len(inspect.signature(fn).parameters)
            if not replace and (fn_name, arity) in self._registered_functions:
                return fn
            kwargs = {}
            registered = False
            if deterministic:
                # Try this, but fall back if sqlite3.NotSupportedError
                try:
                    self.conn.create_function(
                        fn_name, arity, fn, **dict(kwargs, deterministic=True)
                    )
                    registered = True
                except sqlite3.NotSupportedError:
                    pass
            if not registered:
                self.conn.create_function(fn_name, arity, fn, **kwargs)
            self._registered_functions.add((fn_name, arity))
            return fn

        if fn is None:
            return register
        else:
            register(fn)

    def register_fts4_bm25(self):
        "Register the ``rank_bm25(match_info)`` function used for calculating relevance with SQLite FTS4."
        self.register_function(rank_bm25, deterministic=True, replace=True)

    def attach(self, alias: str, filepath: Union[str, pathlib.Path]):
        """
        Attach another SQLite database file to this connection with the specified alias, equivalent to::

            ATTACH DATABASE 'filepath.db' AS alias

        :param alias: Alias name to use
        :param filepath: Path to SQLite database file on disk
        """
        attach_sql = """
            ATTACH DATABASE '{}' AS [{}];
        """.format(
            str(pathlib.Path(filepath).resolve()), alias
        ).strip()
        self.execute(attach_sql)

    def query(
            self, sql: str, params: Optional[Union[Iterable, dict]] = None
    ) -> Generator[dict, None, None]:
        """
        Execute ``sql`` and return an iterable of dictionaries representing each row.

        :param sql: SQL query to execute
        :param params: Parameters to use in that query - an iterable for ``where id = ?``
          parameters, or a dictionary for ``where id = :id``
        """
        cursor = self.conn.execute(sql, params or tuple())
        keys = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(keys, row))

    def execute(
            self, sql: str, parameters: Optional[Union[Iterable, dict]] = None
    ) -> sqlite3.Cursor:
        """
        Execute SQL query and return a ``sqlite3.Cursor``.

        :param sql: SQL query to execute
        :param parameters: Parameters to use in that query - an iterable for ``where id = ?``
          parameters, or a dictionary for ``where id = :id``
        """
        if self._tracer:
            self._tracer(sql, parameters)
        if parameters is not None:
            return self.conn.execute(sql, parameters)
        else:
            return self.conn.execute(sql)

    def executescript(self, sql: str) -> sqlite3.Cursor:
        """
        Execute multiple SQL statements separated by ; and return the ``sqlite3.Cursor``.

        :param sql: SQL to execute
        """
        if self._tracer:
            self._tracer(sql, None)
        return self.conn.executescript(sql)

    def table(self, table_name: str, **kwargs) -> Union["Table", "View"]:
        """
        Return a table object, optionally configured with default options.

        See :ref:`reference_db_table` for option details.

        :param table_name: Name of the table
        """
        if table_name in self.view_names():
            return View(self, table_name, **kwargs)
        else:
            kwargs.setdefault("strict", self.strict)
            return Table(self, table_name, **kwargs)

    def quote(self, value):
        """Duckdb doesn't have SELECT quote, so instead we embed SQLite in it :D :D """
        with sqlite3.connect(":memory:") as db:
            return db.execute(
                # Use SQLite itself to correctly escape this string:
                "SELECT quote(:value)",
                {"value": value},
            ).fetchone()[0]

    def quote_fts(self, query: str) -> str:
        """
        Escape special characters in a SQLite full-text search query.

        This works by surrounding each token within the query with double
        quotes, in order to avoid words like ``NOT`` and ``OR`` having
        special meaning as defined by the FTS query syntax here:

        https://www.sqlite.org/fts5.html#full_text_query_syntax

        If the query has unbalanced ``"`` characters, adds one at end.

        :param query: String to escape
        """
        if query.count('"') % 2:
            query += '"'
        bits = _quote_fts_re.split(query)
        bits = [b for b in bits if b and b != '""']
        return " ".join(
            '"{}"'.format(bit) if not bit.startswith('"') else bit for bit in bits
        )

    def quote_default_value(self, value: str) -> str:
        if any(
                [
                    str(value).startswith("'") and str(value).endswith("'"),
                    str(value).startswith('"') and str(value).endswith('"'),
                ]
        ):
            return value

        if str(value).upper() in ("CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP"):
            return value

        if str(value).endswith(")"):
            # Expr
            return "({})".format(value)

        return self.quote(value)

    def table_names(self, fts4: bool = False, fts5: bool = False) -> List[str]:
        sql = "select table_name from duckdb_tables"
        return [r[0] for r in self.execute(sql).fetchall()]

    def view_names(self) -> List[str]:
        "List of string view names in this database."
        return [
            r[0]
            for r in self.execute(
                "select name from sqlite_master where type = 'view'"
            ).fetchall()
        ]

    @property
    def tables(self) -> List["Table"]:
        "List of Table objects in this database."
        return cast(List["Table"], [self[name] for name in self.table_names()])

    @property
    def views(self) -> List["View"]:
        "List of View objects in this database."
        return cast(List["View"], [self[name] for name in self.view_names()])

    @property
    def triggers(self) -> List[Trigger]:
        "List of ``(name, table_name, sql)`` tuples representing triggers in this database."
        return [
            Trigger(*r)
            for r in self.execute(
                "select name, tbl_name, sql from sqlite_master where type = 'trigger'"
            ).fetchall()
        ]

    @property
    def triggers_dict(self) -> Dict[str, str]:
        "A ``{trigger_name: sql}`` dictionary of triggers in this database."
        return {trigger.name: trigger.sql for trigger in self.triggers}

    @property
    def schema(self) -> str:
        "SQL schema for this database."
        sqls = []
        for row in self.execute(
                "select sql from sqlite_master where sql is not null"
        ).fetchall():
            sql = row[0]
            if not sql.strip().endswith(";"):
                sql += ";"
            sqls.append(sql)
        return "\n".join(sqls)

    @property
    def supports_strict(self) -> bool:
        "Does this database support STRICT mode?"
        try:
            table_name = "t{}".format(secrets.token_hex(16))
            with self.conn:
                self.conn.execute(
                    "create table {} (name text) strict".format(table_name)
                )
                self.conn.execute("drop table {}".format(table_name))
            return True
        except Exception:
            return False

    @property
    def sqlite_version(self) -> Tuple[int, ...]:
        "Version of SQLite, as a tuple of integers for example ``(3, 36, 0)``."
        row = self.execute("select sqlite_version()").fetchall()[0]
        return tuple(map(int, row[0].split(".")))

    @property
    def journal_mode(self) -> str:
        """
        Current ``journal_mode`` of this database.

        https://www.sqlite.org/pragma.html#pragma_journal_mode
        """
        return self.execute("PRAGMA journal_mode;").fetchone()[0]

    def enable_wal(self):
        """
        Sets ``journal_mode`` to ``'wal'`` to enable Write-Ahead Log mode.
        """
        if self.journal_mode != "wal":
            with self.ensure_autocommit_off():
                self.execute("PRAGMA journal_mode=wal;")

    def disable_wal(self):
        "Sets ``journal_mode`` back to ``'delete'`` to disable Write-Ahead Log mode."
        if self.journal_mode != "delete":
            with self.ensure_autocommit_off():
                self.execute("PRAGMA journal_mode=delete;")

    def _ensure_counts_table(self):
        with self.conn:
            self.execute(_COUNTS_TABLE_CREATE_SQL.format(self._counts_table_name))

    def enable_counts(self):
        """
        Enable trigger-based count caching for every table in the database, see
        :ref:`python_api_cached_table_counts`.
        """
        self._ensure_counts_table()
        for table in self.tables:
            if (
                    table.virtual_table_using is None
                    and table.name != self._counts_table_name
            ):
                table.enable_counts()
        self.use_counts_table = True

    def cached_counts(self, tables: Optional[Iterable[str]] = None) -> Dict[str, int]:
        """
        Return ``{table_name: count}`` dictionary of cached counts for specified tables, or
        all tables if ``tables`` not provided.

        :param tables: Subset list of tables to return counts for.
        """
        sql = "select [table], count from {}".format(self._counts_table_name)
        if tables:
            sql += " where [table] in ({})".format(", ".join("?" for table in tables))
        try:
            return {r[0]: r[1] for r in self.execute(sql, tables).fetchall()}
        except OperationalError:
            return {}

    def reset_counts(self):
        "Re-calculate cached counts for tables."
        tables = [table for table in self.tables if table.has_counts_triggers]
        with self.conn:
            self._ensure_counts_table()
            counts_table = self[self._counts_table_name]
            counts_table.delete_where()
            counts_table.insert_all(
                {"table": table.name, "count": table.execute_count()}
                for table in tables
            )

    def execute_returning_dicts(
            self, sql: str, params: Optional[Union[Iterable, dict]] = None
    ) -> List[dict]:
        return list(self.query(sql, params))

    def resolve_foreign_keys(
            self, name: str, foreign_keys: ForeignKeysType
    ) -> List[ForeignKey]:
        """
        Given a list of differing foreign_keys definitions, return a list of
        fully resolved ForeignKey() named tuples.

        :param name: Name of table that foreign keys are being defined for
        :param foreign_keys: List of foreign keys, each of which can be a
            string, a ForeignKey() named tuple, a tuple of (column, other_table),
            or a tuple of (column, other_table, other_column), or a tuple of
            (table, column, other_table, other_column)
        """
        table = cast(Table, self[name])
        if all(isinstance(fk, ForeignKey) for fk in foreign_keys):
            return cast(List[ForeignKey], foreign_keys)
        if all(isinstance(fk, str) for fk in foreign_keys):
            # It's a list of columns
            fks = []
            for column in foreign_keys:
                column = cast(str, column)
                other_table = table.guess_foreign_table(column)
                other_column = table.guess_foreign_column(other_table)
                fks.append(ForeignKey(name, column, other_table, other_column))
            return fks
        assert all(
            isinstance(fk, (tuple, list)) for fk in foreign_keys
        ), "foreign_keys= should be a list of tuples"
        fks = []
        for tuple_or_list in foreign_keys:
            if len(tuple_or_list) == 4:
                assert (
                        tuple_or_list[0] == name
                ), "First item in {} should have been {}".format(tuple_or_list, name)
            assert len(tuple_or_list) in (
                2,
                3,
                4,
            ), "foreign_keys= should be a list of tuple pairs or triples"
            if len(tuple_or_list) in (3, 4):
                if len(tuple_or_list) == 4:
                    tuple_or_list = cast(Tuple[str, str, str], tuple_or_list[1:])
                else:
                    tuple_or_list = cast(Tuple[str, str, str], tuple_or_list)
                fks.append(
                    ForeignKey(
                        name, tuple_or_list[0], tuple_or_list[1], tuple_or_list[2]
                    )
                )
            else:
                # Guess the primary key
                fks.append(
                    ForeignKey(
                        name,
                        tuple_or_list[0],
                        tuple_or_list[1],
                        table.guess_foreign_column(tuple_or_list[1]),
                    )
                )
        return fks

    def create_table_sql(
            self,
            name: str,
            columns: Dict[str, Any],
            pk: Optional[Any] = None,
            foreign_keys: Optional[ForeignKeysType] = None,
            column_order: Optional[List[str]] = None,
            not_null: Optional[Iterable[str]] = None,
            defaults: Optional[Dict[str, Any]] = None,
            hash_id: Optional[str] = None,
            hash_id_columns: Optional[Iterable[str]] = None,
            extracts: Optional[Union[Dict[str, str], List[str]]] = None,
            if_not_exists: bool = False,
            strict: bool = False,
    ) -> str:
        """
        Returns the SQL ``CREATE TABLE`` statement for creating the specified table.

        :param name: Name of table
        :param columns: Dictionary mapping column names to their types, for example ``{"name": str, "age": int}``
        :param pk: String name of column to use as a primary key, or a tuple of strings for a compound primary key covering multiple columns
        :param foreign_keys: List of foreign key definitions for this table
        :param column_order: List specifying which columns should come first
        :param not_null: List of columns that should be created as ``NOT NULL``
        :param defaults: Dictionary specifying default values for columns
        :param hash_id: Name of column to be used as a primary key containing a hash of the other columns
        :param hash_id_columns: List of columns to be used when calculating the hash ID for a row
        :param extracts: List or dictionary of columns to be extracted during inserts, see :ref:`python_api_extracts`
        :param if_not_exists: Use ``CREATE TABLE IF NOT EXISTS``
        :param strict: Apply STRICT mode to table
        """
        if hash_id_columns and (hash_id is None):
            hash_id = "id"
        foreign_keys = self.resolve_foreign_keys(name, foreign_keys or [])
        foreign_keys_by_column = {fk.column: fk for fk in foreign_keys}
        # any extracts will be treated as integer columns with a foreign key
        extracts = resolve_extracts(extracts)
        for extract_column, extract_table in extracts.items():
            if isinstance(extract_column, tuple):
                assert False
            # Ensure other table exists
            if not self[extract_table].exists():
                self.create_table(extract_table, {"id": int, "value": str}, pk="id")
            columns[extract_column] = int
            foreign_keys_by_column[extract_column] = ForeignKey(
                name, extract_column, extract_table, "id"
            )
        # Soundness check not_null, and defaults if provided
        not_null = not_null or set()
        defaults = defaults or {}
        assert columns, "Tables must have at least one column"
        assert all(
            n in columns for n in not_null
        ), "not_null set {} includes items not in columns {}".format(
            repr(not_null), repr(set(columns.keys()))
        )
        assert all(
            n in columns for n in defaults
        ), "defaults set {} includes items not in columns {}".format(
            repr(set(defaults)), repr(set(columns.keys()))
        )
        validate_column_names(columns.keys())
        column_items = list(columns.items())
        if column_order is not None:
            def sort_key(p):
                return column_order.index(p[0]) if p[0] in column_order else 999

            column_items.sort(key=sort_key)
        if hash_id:
            column_items.insert(0, (hash_id, str))
            pk = hash_id
        # Soundness check foreign_keys point to existing tables
        for fk in foreign_keys:
            if fk.other_table == name and columns.get(fk.other_column):
                continue
            if fk.other_column != "rowid" and not any(
                    c for c in self[fk.other_table].columns if c.name == fk.other_column
            ):
                raise AlterError(
                    "No such column: {}.{}".format(fk.other_table, fk.other_column)
                )

        column_defs = []
        # ensure pk is a tuple
        single_pk = None
        if isinstance(pk, list) and len(pk) == 1 and isinstance(pk[0], str):
            pk = pk[0]
        if isinstance(pk, str):
            single_pk = pk
            if pk not in [c[0] for c in column_items]:
                column_items.insert(0, (pk, int))
        for column_name, column_type in column_items:
            column_extras = []
            if column_name == single_pk:
                column_extras.append("PRIMARY KEY")
            if column_name in not_null:
                column_extras.append("NOT NULL")
            if column_name in defaults and defaults[column_name] is not None:
                column_extras.append(
                    "DEFAULT {}".format(self.quote_default_value(defaults[column_name]))
                )
            if column_name in foreign_keys_by_column:
                column_extras.append(
                    "REFERENCES \"{other_table}\"(\"{other_column}\")".format(
                        other_table=foreign_keys_by_column[column_name].other_table,
                        other_column=foreign_keys_by_column[column_name].other_column,
                    )
                )
            column_defs.append(
                "   \"{column_name}\" {column_type}{column_extras}".format(
                    column_name=column_name,
                    column_type=COLUMN_TYPE_MAPPING[column_type],
                    column_extras=(
                        (" " + " ".join(column_extras)) if column_extras else ""
                    ),
                )
            )
        extra_pk = ""
        if single_pk is None and pk and len(pk) > 1:
            extra_pk = ",\n   PRIMARY KEY ({pks})".format(
                pks=", ".join(["\"{}\"".format(p) for p in pk])
            )
        columns_sql = ",\n".join(column_defs)
        sql = """CREATE TABLE {if_not_exists}\"{table}\" (
{columns_sql}{extra_pk}
){strict};
        """.format(
            if_not_exists="IF NOT EXISTS " if if_not_exists else "",
            table=name,
            columns_sql=columns_sql,
            extra_pk=extra_pk,
            strict=" STRICT" if strict and self.supports_strict else "",
        )
        return sql

    def create_table(
            self,
            name: str,
            columns: Dict[str, Any],
            pk: Optional[Any] = None,
            foreign_keys: Optional[ForeignKeysType] = None,
            column_order: Optional[List[str]] = None,
            not_null: Optional[Iterable[str]] = None,
            defaults: Optional[Dict[str, Any]] = None,
            hash_id: Optional[str] = None,
            hash_id_columns: Optional[Iterable[str]] = None,
            extracts: Optional[Union[Dict[str, str], List[str]]] = None,
            if_not_exists: bool = False,
            replace: bool = False,
            ignore: bool = False,
            transform: bool = False,
            strict: bool = False,
    ) -> "Table":
        """
        Create a table with the specified name and the specified ``{column_name: type}`` columns.

        See :ref:`python_api_explicit_create`.

        :param name: Name of table
        :param columns: Dictionary mapping column names to their types, for example ``{"name": str, "age": int}``
        :param pk: String name of column to use as a primary key, or a tuple of strings for a compound primary key covering multiple columns
        :param foreign_keys: List of foreign key definitions for this table
        :param column_order: List specifying which columns should come first
        :param not_null: List of columns that should be created as ``NOT NULL``
        :param defaults: Dictionary specifying default values for columns
        :param hash_id: Name of column to be used as a primary key containing a hash of the other columns
        :param hash_id_columns: List of columns to be used when calculating the hash ID for a row
        :param extracts: List or dictionary of columns to be extracted during inserts, see :ref:`python_api_extracts`
        :param if_not_exists: Use ``CREATE TABLE IF NOT EXISTS``
        :param replace: Drop and replace table if it already exists
        :param ignore: Silently do nothing if table already exists
        :param transform: If table already exists transform it to fit the specified schema
        :param strict: Apply STRICT mode to table
        """
        # Transform table to match the new definition if table already exists:
        if self[name].exists():
            if ignore:
                return cast(Table, self[name])
            elif replace:
                self[name].drop()
        if transform and self[name].exists():
            table = cast(Table, self[name])
            should_transform = False
            # First add missing columns and figure out columns to drop
            existing_columns = table.columns_dict
            missing_columns = dict(
                (col_name, col_type)
                for col_name, col_type in columns.items()
                if col_name not in existing_columns
            )
            columns_to_drop = [
                column for column in existing_columns if column not in columns
            ]
            if missing_columns:
                for col_name, col_type in missing_columns.items():
                    table.add_column(col_name, col_type)
            if missing_columns or columns_to_drop or columns != existing_columns:
                should_transform = True
            # Do we need to change the column order?
            if (
                    column_order
                    and list(existing_columns)[: len(column_order)] != column_order
            ):
                should_transform = True
            # Has the primary key changed?
            current_pks = table.pks
            desired_pk = None
            if isinstance(pk, str):
                desired_pk = [pk]
            elif pk:
                desired_pk = list(pk)
            if desired_pk and current_pks != desired_pk:
                should_transform = True
            # Any not-null changes?
            current_not_null = {c.name for c in table.columns if c.notnull}
            desired_not_null = set(not_null) if not_null else set()
            if current_not_null != desired_not_null:
                should_transform = True
            # How about defaults?
            if defaults and defaults != table.default_values:
                should_transform = True
            # Only run .transform() if there is something to do
            if should_transform:
                table.transform(
                    types=columns,
                    drop=columns_to_drop,
                    column_order=column_order,
                    not_null=not_null,
                    defaults=defaults,
                    pk=pk,
                )
            return table
        sql = self.create_table_sql(
            name=name,
            columns=columns,
            pk=pk,
            foreign_keys=foreign_keys,
            column_order=column_order,
            not_null=not_null,
            defaults=defaults,
            hash_id=hash_id,
            hash_id_columns=hash_id_columns,
            extracts=extracts,
            if_not_exists=if_not_exists,
            strict=strict,
        )
        self.execute(sql)
        created_table = self.table(
            name,
            pk=pk,
            foreign_keys=foreign_keys,
            column_order=column_order,
            not_null=not_null,
            defaults=defaults,
            hash_id=hash_id,
            hash_id_columns=hash_id_columns,
        )
        return cast(Table, created_table)

    def rename_table(self, name: str, new_name: str):
        """
        Rename a table.

        :param name: Current table name
        :param new_name: Name to rename it to
        """
        self.execute(
            "ALTER TABLE [{name}] RENAME TO [{new_name}]".format(
                name=name, new_name=new_name
            )
        )

    def create_view(
            self, name: str, sql: str, ignore: bool = False, replace: bool = False
    ):
        """
        Create a new SQL view with the specified name - ``sql`` should start with ``SELECT ...``.

        :param name: Name of the view
        :param sql: SQL ``SELECT`` query to use for this view.
        :param ignore: Set to ``True`` to do nothing if a view with this name already exists
        :param replace: Set to ``True`` to replace the view if one with this name already exists
        """
        assert not (
                ignore and replace
        ), "Use one or the other of ignore/replace, not both"
        create_sql = "CREATE VIEW {name} AS {sql}".format(name=name, sql=sql)
        if ignore or replace:
            # Does view exist already?
            if name in self.view_names():
                if ignore:
                    return self
                elif replace:
                    # If SQL is the same, do nothing
                    if create_sql == self[name].schema:
                        return self
                    self[name].drop()
        self.execute(create_sql)
        return self

    def m2m_table_candidates(self, table: str, other_table: str) -> List[str]:
        """
        Given two table names returns the name of tables that could define a
        many-to-many relationship between those two tables, based on having
        foreign keys to both of the provided tables.

        :param table: Table name
        :param other_table: Other table name
        """
        candidates = []
        tables = {table, other_table}
        for table_obj in self.tables:
            # Does it have foreign keys to both table and other_table?
            has_fks_to = {fk.other_table for fk in table_obj.foreign_keys}
            if has_fks_to.issuperset(tables):
                candidates.append(table_obj.name)
        return candidates

    def add_foreign_keys(self, foreign_keys: Iterable[Tuple[str, str, str, str]]):
        """
        See :ref:`python_api_add_foreign_keys`.

        :param foreign_keys: A list of  ``(table, column, other_table, other_column)``
          tuples
        """
        # foreign_keys is a list of explicit 4-tuples
        assert all(
            len(fk) == 4 and isinstance(fk, (list, tuple)) for fk in foreign_keys
        ), "foreign_keys must be a list of 4-tuples, (table, column, other_table, other_column)"

        foreign_keys_to_create = []

        # Verify that all tables and columns exist
        for table, column, other_table, other_column in foreign_keys:
            if not self[table].exists():
                raise AlterError("No such table: {}".format(table))
            table_obj = self[table]
            if not isinstance(table_obj, Table):
                raise AlterError("Must be a table, not a view: {}".format(table))
            table_obj = cast(Table, table_obj)
            if column not in table_obj.columns_dict:
                raise AlterError("No such column: {} in {}".format(column, table))
            if not self[other_table].exists():
                raise AlterError("No such other_table: {}".format(other_table))
            if (
                    other_column != "rowid"
                    and other_column not in self[other_table].columns_dict
            ):
                raise AlterError(
                    "No such other_column: {} in {}".format(other_column, other_table)
                )
            # We will silently skip foreign keys that exist already
            if not any(
                    fk
                    for fk in table_obj.foreign_keys
                    if fk.column == column
                       and fk.other_table == other_table
                       and fk.other_column == other_column
            ):
                foreign_keys_to_create.append(
                    (table, column, other_table, other_column)
                )

        # Group them by table
        by_table: Dict[str, List] = {}
        for fk in foreign_keys_to_create:
            by_table.setdefault(fk[0], []).append(fk)

        for table, fks in by_table.items():
            cast(Table, self[table]).transform(add_foreign_keys=fks)

        self.vacuum()

    def index_foreign_keys(self):
        "Create indexes for every foreign key column on every table in the database."
        for table_name in self.table_names():
            table = self[table_name]
            existing_indexes = {
                i.columns[0] for i in table.indexes if len(i.columns) == 1
            }
            for fk in table.foreign_keys:
                if fk.column not in existing_indexes:
                    table.create_index([fk.column], find_unique_name=True)

    def vacuum(self):
        "Run a SQLite ``VACUUM`` against the database."
        self.execute("VACUUM;")

    def analyze(self, name=None):
        """
        Run ``ANALYZE`` against the entire database or a named table or index.

        :param name: Run ``ANALYZE`` against this specific named table or index
        """
        sql = "ANALYZE"
        if name is not None:
            sql += " [{}]".format(name)
        self.execute(sql)

    def iterdump(self) -> Generator[str, None, None]:
        "A sequence of strings representing a SQL dump of the database"
        if iterdump:
            yield from iterdump(self.conn)
        else:
            try:
                yield from self.conn.iterdump()
            except AttributeError:
                raise AttributeError(
                    "conn.iterdump() not found - try pip install sqlite-dump"
                )

    def init_spatialite(self, path: Optional[str] = None) -> bool:
        """
        The ``init_spatialite`` method will load and initialize the SpatiaLite extension.
        The ``path`` argument should be an absolute path to the compiled extension, which
        can be found using ``find_spatialite``.

        Returns ``True`` if SpatiaLite was successfully initialized.

        .. code-block:: python

            from sqlite_utils.db import Database
            from sqlite_utils.utils import find_spatialite

            db = Database("mydb.db")
            db.init_spatialite(find_spatialite())

        If you've installed SpatiaLite somewhere unexpected (for testing an alternate version, for example)
        you can pass in an absolute path:

        .. code-block:: python

            from sqlite_utils.db import Database
            from sqlite_utils.utils import find_spatialite

            db = Database("mydb.db")
            db.init_spatialite("./local/mod_spatialite.dylib")

        :param path: Path to SpatiaLite module on disk
        """
        if path is None:
            path = find_spatialite()

        self.conn.enable_load_extension(True)
        self.conn.load_extension(path)
        # Initialize SpatiaLite if not yet initialized
        if "spatial_ref_sys" in self.table_names():
            return False
        cursor = self.execute("select InitSpatialMetadata(1)")
        result = cursor.fetchone()
        return result and bool(result[0])


class DuckDBQueryable(Queryable):
    def pks_and_rows_where(
            self,
            where: Optional[str] = None,
            where_args: Optional[Union[Iterable, dict]] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> Generator[Tuple[Any, Dict], None, None]:
        column_names = [column.name for column in self.columns]
        pks = [column.name for column in self.columns if column.is_pk]
        if not pks:
            column_names.insert(0, "rowid")
            pks = ["rowid"]
        select = ",".join("\"{}\"".format(column_name) for column_name in column_names)
        for row in self.rows_where(
                select=select,
                where=where,
                where_args=where_args,
                order_by=order_by,
                limit=limit,
                offset=offset,
        ):
            row_pk = tuple(row[pk] for pk in pks)
            if len(row_pk) == 1:
                row_pk = row_pk[0]
            yield row_pk, row

    def rows_where(
            self,
            where: Optional[str] = None,
            where_args: Optional[Union[Iterable, dict]] = None,
            order_by: Optional[str] = None,
            select: str = "*",
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Iterate over every row in this table or view that matches the specified where clause.

        Returns each row as a dictionary. See :ref:`python_api_rows` for more details.

        :param where: SQL where fragment to use, for example ``id > ?``
        :param where_args: Parameters to use with that fragment - an iterable for ``id > ?``
          parameters, or a dictionary for ``id > :id``
        :param order_by: Column or fragment of SQL to order by
        :param select: Comma-separated list of columns to select - defaults to ``*``
        :param limit: Integer number of rows to limit to
        :param offset: Integer for SQL offset
        """
        if not self.exists():
            return
        sql = "select {} from \"{}\"".format(select, self.name)
        if where is not None:
            sql += " where " + where
        if order_by is not None:
            sql += " order by " + order_by
        if limit is not None:
            sql += " limit {}".format(limit)
        if offset is not None:
            sql += " offset {}".format(offset)
        cursor = self.db.execute(sql, where_args or [])
        columns = [c[0] for c in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))

    def count_where(
        self,
        where: Optional[str] = None,
        where_args: Optional[Union[Iterable, dict]] = None,
    ) -> int:
        sql = "select count(*) from \"{}\"".format(self.name)
        if where is not None:
            sql += " where " + where
        return self.db.execute(sql, where_args or []).fetchone()[0]


Queryable.rows_where = DuckDBQueryable.rows_where
Queryable.count_where = DuckDBQueryable.count_where
Queryable.pks_and_rows_where = DuckDBQueryable.pks_and_rows_where

class DuckDBTable(Table):
    @property
    def schema(self) -> str:
        "SQL schema for this table or view."
        return self.db.execute(
            "select sql from duckdb_tables where table_name = ?", (self.name,)
        ).fetchone()[0]

    @property
    def columns(self) -> List["Column"]:
        "List of :ref:`Columns <reference_db_other_column>` representing the columns in this table or view."
        if not self.exists():
            return []
        rows = self.db.execute("PRAGMA table_info(\"{}\")".format(self.name)).fetchall()
        return [Column(*row) for row in rows]

    def build_insert_queries_and_params(
            self,
            extracts,
            chunk,
            all_columns,
            hash_id,
            hash_id_columns,
            upsert,
            pk,
            not_null,
            conversions,
            num_records_processed,
            replace,
            ignore,
    ):
        # values is the list of insert data that is passed to the
        # .execute() method - but some of them may be replaced by
        # new primary keys if we are extracting any columns.
        values = []
        if hash_id_columns and hash_id is None:
            hash_id = "id"
        extracts = resolve_extracts(extracts)
        for record in chunk:
            record_values = []
            for key in all_columns:
                value = jsonify_if_needed(
                    record.get(
                        key,
                        (
                            None
                            if key != hash_id
                            else hash_record(record, hash_id_columns)
                        ),
                    )
                )
                if key in extracts:
                    extract_table = extracts[key]
                    value = self.db[extract_table].lookup({"value": value})
                record_values.append(value)
            values.append(record_values)

        queries_and_params = []
        if upsert:
            if isinstance(pk, str):
                pks = [pk]
            else:
                pks = pk
            self.last_pk = None
            for record_values in values:
                record = dict(zip(all_columns, record_values))
                placeholders = list(pks)
                # Need to populate not-null columns too, or INSERT OR IGNORE ignores
                # them since it ignores the resulting integrity errors
                if not_null:
                    placeholders.extend(not_null)
                sql = "INSERT OR IGNORE INTO \"{table}\"({cols}) VALUES({placeholders});".format(
                    table=self.name,
                    cols=", ".join(["\"{}\"".format(p) for p in placeholders]),
                    placeholders=", ".join(["?" for p in placeholders]),
                )
                queries_and_params.append(
                    (sql, [record[col] for col in pks] + ["" for _ in (not_null or [])])
                )
                # UPDATE [book] SET [name] = 'Programming' WHERE [id] = 1001;
                set_cols = [col for col in all_columns if col not in pks]
                if set_cols:
                    sql2 = "UPDATE [{table}] SET {pairs} WHERE {wheres}".format(
                        table=self.name,
                        pairs=", ".join(
                            "[{}] = {}".format(col, conversions.get(col, "?"))
                            for col in set_cols
                        ),
                        wheres=" AND ".join("[{}] = ?".format(pk) for pk in pks),
                    )
                    queries_and_params.append(
                        (
                            sql2,
                            [record[col] for col in set_cols]
                            + [record[pk] for pk in pks],
                        )
                    )
                # We can populate .last_pk right here
                if num_records_processed == 1:
                    self.last_pk = tuple(record[pk] for pk in pks)
                    if len(self.last_pk) == 1:
                        self.last_pk = self.last_pk[0]

        else:
            or_what = ""
            if replace:
                or_what = "OR REPLACE "
            elif ignore:
                or_what = "OR IGNORE "
            sql = """
                INSERT {or_what}INTO \"{table}\" ({columns}) VALUES {rows};
            """.strip().format(
                or_what=or_what,
                table=self.name,
                columns=", ".join("\"{}\"".format(c) for c in all_columns),
                rows=", ".join(
                    "({placeholders})".format(
                        placeholders=", ".join(
                            [conversions.get(col, "?") for col in all_columns]
                        )
                    )
                    for record in chunk
                ),
            )
            flat_values = list(itertools.chain(*values))
            queries_and_params = [(sql, flat_values)]

        return queries_and_params

    def insert_chunk(
            self,
            alter,
            extracts,
            chunk,
            all_columns,
            hash_id,
            hash_id_columns,
            upsert,
            pk,
            not_null,
            conversions,
            num_records_processed,
            replace,
            ignore,
    ):
        queries_and_params = self.build_insert_queries_and_params(
            extracts,
            chunk,
            all_columns,
            hash_id,
            hash_id_columns,
            upsert,
            pk,
            not_null,
            conversions,
            num_records_processed,
            replace,
            ignore,
        )

        with self.db.conn:
            result = None
            for query, params in queries_and_params:
                try:
                    result = self.db.execute(query, params)
                except OperationalError as e:
                    if alter and (" column" in e.args[0]):
                        # Attempt to add any missing columns, then try again
                        self.add_missing_columns(chunk)
                        result = self.db.execute(query, params)
                    elif e.args[0] == "too many SQL variables":
                        first_half = chunk[: len(chunk) // 2]
                        second_half = chunk[len(chunk) // 2 :]

                        self.insert_chunk(
                            alter,
                            extracts,
                            first_half,
                            all_columns,
                            hash_id,
                            hash_id_columns,
                            upsert,
                            pk,
                            not_null,
                            conversions,
                            num_records_processed,
                            replace,
                            ignore,
                        )

                        self.insert_chunk(
                            alter,
                            extracts,
                            second_half,
                            all_columns,
                            hash_id,
                            hash_id_columns,
                            upsert,
                            pk,
                            not_null,
                            conversions,
                            num_records_processed,
                            replace,
                            ignore,
                        )

                    else:
                        raise
            if num_records_processed == 1 and not upsert:
                self.last_rowid = 0 #result.lastrowid #TODO: fixme
                self.last_pk = self.last_rowid
                # self.last_rowid will be 0 if a "INSERT OR IGNORE" happened
                if (hash_id or pk) and self.last_rowid:
                    row = list(self.rows_where("rowid = ?", [self.last_rowid]))[0]
                    if hash_id:
                        self.last_pk = row[hash_id]
                    elif isinstance(pk, str):
                        self.last_pk = row[pk]
                    else:
                        self.last_pk = tuple(row[p] for p in pk)

        return

    def insert(
        self,
        record: Dict[str, Any],
        pk=DEFAULT,
        foreign_keys=DEFAULT,
        column_order: Optional[Union[List[str], Default]] = DEFAULT,
        not_null: Optional[Union[Iterable[str], Default]] = DEFAULT,
        defaults: Optional[Union[Dict[str, Any], Default]] = DEFAULT,
        hash_id: Optional[Union[str, Default]] = DEFAULT,
        hash_id_columns: Optional[Union[Iterable[str], Default]] = DEFAULT,
        alter: Optional[Union[bool, Default]] = DEFAULT,
        ignore: Optional[Union[bool, Default]] = DEFAULT,
        replace: Optional[Union[bool, Default]] = DEFAULT,
        extracts: Optional[Union[Dict[str, str], List[str], Default]] = DEFAULT,
        conversions: Optional[Union[Dict[str, str], Default]] = DEFAULT,
        columns: Optional[Union[Dict[str, Any], Default]] = DEFAULT,
        strict: Optional[Union[bool, Default]] = DEFAULT,
    ) -> "DuckDBTable":
        pass

    def insert_all(
        self,
        records,
        pk=DEFAULT,
        foreign_keys=DEFAULT,
        column_order=DEFAULT,
        not_null=DEFAULT,
        defaults=DEFAULT,
        batch_size=DEFAULT,
        hash_id=DEFAULT,
        hash_id_columns=DEFAULT,
        alter=DEFAULT,
        ignore=DEFAULT,
        replace=DEFAULT,
        truncate=False,
        extracts=DEFAULT,
        conversions=DEFAULT,
        columns=DEFAULT,
        upsert=False,
        analyze=False,
        strict=DEFAULT,
    ) -> "DuckDBTable":
        """
        Like ``.insert()`` but takes a list of records and ensures that the table
        that it creates (if table does not exist) has columns for ALL of that data.

        Use ``analyze=True`` to run ``ANALYZE`` after the insert has completed.
        """
        pk = self.value_or_default("pk", pk)
        foreign_keys = self.value_or_default("foreign_keys", foreign_keys)
        column_order = self.value_or_default("column_order", column_order)
        not_null = self.value_or_default("not_null", not_null)
        defaults = self.value_or_default("defaults", defaults)
        batch_size = self.value_or_default("batch_size", batch_size)
        hash_id = self.value_or_default("hash_id", hash_id)
        hash_id_columns = self.value_or_default("hash_id_columns", hash_id_columns)
        alter = self.value_or_default("alter", alter)
        ignore = self.value_or_default("ignore", ignore)
        replace = self.value_or_default("replace", replace)
        extracts = self.value_or_default("extracts", extracts)
        conversions = self.value_or_default("conversions", conversions) or {}
        columns = self.value_or_default("columns", columns)
        strict = self.value_or_default("strict", strict)

        if hash_id_columns and hash_id is None:
            hash_id = "id"

        if upsert and (not pk and not hash_id):
            raise PrimaryKeyRequired("upsert() requires a pk")
        assert not (hash_id and pk), "Use either pk= or hash_id="
        if hash_id_columns and (hash_id is None):
            hash_id = "id"
        if hash_id:
            pk = hash_id

        assert not (
                ignore and replace
        ), "Use either ignore=True or replace=True, not both"
        all_columns = []
        first = True
        num_records_processed = 0
        # Fix up any records with square braces in the column names
        records = fix_square_braces(records)
        # We can only handle a max of 999 variables in a SQL insert, so
        # we need to adjust the batch_size down if we have too many cols
        records = iter(records)
        # Peek at first record to count its columns:
        try:
            first_record = next(records)
        except StopIteration:
            return self  # It was an empty list
        num_columns = len(first_record.keys())
        assert (
                num_columns <= SQLITE_MAX_VARS
        ), "Rows can have a maximum of {} columns".format(SQLITE_MAX_VARS)
        batch_size = max(1, min(batch_size, SQLITE_MAX_VARS // num_columns))
        self.last_rowid = None
        self.last_pk = None
        if truncate and self.exists():
            self.db.execute("DELETE FROM [{}];".format(self.name))
        for chunk in chunks(itertools.chain([first_record], records), batch_size):
            chunk = list(chunk)
            num_records_processed += len(chunk)
            if first:
                if not self.exists():
                    # Use the first batch to derive the table names
                    column_types = suggest_column_types(chunk)
                    column_types.update(columns or {})
                    self.create(
                        column_types,
                        pk,
                        foreign_keys,
                        column_order=column_order,
                        not_null=not_null,
                        defaults=defaults,
                        hash_id=hash_id,
                        hash_id_columns=hash_id_columns,
                        extracts=extracts,
                        strict=strict,
                    )
                all_columns_set = set()
                for record in chunk:
                    all_columns_set.update(record.keys())
                all_columns = list(sorted(all_columns_set))
                if hash_id:
                    all_columns.insert(0, hash_id)
            else:
                for record in chunk:
                    all_columns += [
                        column for column in record if column not in all_columns
                    ]

            first = False

            self.insert_chunk(
                alter,
                extracts,
                chunk,
                all_columns,
                hash_id,
                hash_id_columns,
                upsert,
                pk,
                not_null,
                conversions,
                num_records_processed,
                replace,
                ignore,
            )

        if analyze:
            self.analyze()

        return self


Table.schema = DuckDBTable.schema
Table.columns = DuckDBTable.columns
Table.build_insert_queries_and_params = DuckDBTable.build_insert_queries_and_params
Table.insert_chunk = DuckDBTable.insert_chunk
Table.schema = DuckDBTable.schema
Table.insert_all = DuckDBTable.insert_all
Table.insert_chunk = DuckDBTable.insert_chunk


class DuckDBView(View):
    pass
