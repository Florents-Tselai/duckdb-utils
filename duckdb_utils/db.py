import os
import pathlib
from typing import (
    Callable,
    Union,
    Optional,
    List,
    Dict, Any,
    Iterable
)

import duckdb
import sqlite_utils
from sqlite_utils.db import (Table, ForeignKeysType, resolve_extracts, validate_column_names, COLUMN_TYPE_MAPPING,
                             jsonify_if_needed, ForeignKey, AlterError, Queryable, View, Column)
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
)

import itertools

######### Monkey-patching #########
_COUNTS_TABLE_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS "{}"(
   "table" TEXT PRIMARY KEY,
   count INTEGER DEFAULT 0
);
""".strip()


class DuckDBQueryable(Queryable):
    pass

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

Table.schema = DuckDBTable.schema
Table.columns = DuckDBTable.columns

class DuckDBView(View):
    pass


###################################

class DatabaseAPI(sqlite_utils.Database):
    """For future extraction into a library"""


class Database(DatabaseAPI):

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

    def _ensure_counts_table(self):
        with self.conn:
            self.execute(_COUNTS_TABLE_CREATE_SQL.format(self._counts_table_name))

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

    def table_names(self, fts4: bool = False, fts5: bool = False) -> List[str]:
        sql = "select table_name from duckdb_tables"
        return [r[0] for r in self.execute(sql).fetchall()]

    def quote(self, value):
        """Duckdb doesn't have SELECT quote, so instead f"""
        with sqlite3.connect(":memory:") as db:
            return db.execute(
                # Use SQLite itself to correctly escape this string:
                "SELECT quote(:value)",
                {"value": value},
            ).fetchone()[0]
