import io
import itertools
import json

import click
import tabulate
from click_default_group import DefaultGroup  # type: ignore
from sqlite_utils.cli import (output_rows,
                              VALID_COLUMN_TYPES, UNICODE_ERROR, verify_is_dict, _find_variables)
import csv as csv_std

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
    _flatten, _compile_code, file_progress, TypeTracker, decode_base64_values
)

import duckdb_utils
import sys

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def output_options(fn):
    for decorator in reversed(
            (
                    click.option(
                        "--nl",
                        help="Output newline-delimited JSON",
                        is_flag=True,
                        default=False,
                    ),
                    click.option(
                        "--arrays",
                        help="Output rows as arrays instead of objects",
                        is_flag=True,
                        default=False,
                    ),
                    click.option("--csv", is_flag=True, help="Output CSV"),
                    click.option("--tsv", is_flag=True, help="Output TSV"),
                    click.option("--no-headers", is_flag=True, help="Omit CSV headers"),
                    click.option(
                        "-t", "--table", is_flag=True, help="Output as a formatted table"
                    ),
                    click.option(
                        "--fmt",
                        help="Table format - one of {}".format(
                            ", ".join(tabulate.tabulate_formats)
                        ),
                    ),
                    click.option(
                        "--json-cols",
                        help="Detect JSON cols and output them as JSON, not escaped strings",
                        is_flag=True,
                        default=False,
                    ),
            )
    ):
        fn = decorator(fn)
    return fn


def load_extension_option(fn):
    return click.option(
        "--load-extension",
        multiple=True,
        help="Path to SQLite extension, with optional :entrypoint",
    )(fn)


@click.group(
    cls=DefaultGroup,
    default="query",
    default_if_no_args=True,
    context_settings=CONTEXT_SETTINGS,
)
@click.version_option()
def cli():
    "Commands for interacting with a DuckDB database"
    pass


@cli.command()
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "--fts4", help="Just show FTS4 enabled tables", default=False, is_flag=True
)
@click.option(
    "--fts5", help="Just show FTS5 enabled tables", default=False, is_flag=True
)
@click.option(
    "--counts", help="Include row counts per table", default=False, is_flag=True
)
@output_options
@click.option(
    "--columns",
    help="Include list of columns for each table",
    is_flag=True,
    default=False,
)
@click.option(
    "--schema",
    help="Include schema for each table",
    is_flag=True,
    default=False,
)
@load_extension_option
def tables(
        path,
        fts4,
        fts5,
        counts,
        nl,
        arrays,
        csv,
        tsv,
        no_headers,
        table,
        fmt,
        json_cols,
        columns,
        schema,
        load_extension,
        views=False,
):
    """List the tables in the database

    Example:

    \b
        sqlite-utils tables trees.db
    """
    db = duckdb_utils.Database(path)
    _load_extensions(db, load_extension)
    headers = ["view" if views else "table"]
    if counts:
        headers.append("count")
    if columns:
        headers.append("columns")
    if schema:
        headers.append("schema")

    def _iter():
        if views:
            items = db.view_names()
        else:
            items = db.table_names(fts4=fts4, fts5=fts5)
        for name in items:
            row = [name]
            if counts:
                row.append(db[name].count)
            if columns:
                cols = [c.name for c in db[name].columns]
                if csv:
                    row.append("\n".join(cols))
                else:
                    row.append(cols)
            if schema:
                row.append(db[name].schema)
            yield row

    if table or fmt:
        print(tabulate.tabulate(_iter(), headers=headers, tablefmt=fmt or "simple"))
    elif csv or tsv:
        writer = csv_std.writer(sys.stdout, dialect="excel-tab" if tsv else "excel")
        if not no_headers:
            writer.writerow(headers)
        for row in _iter():
            writer.writerow(row)
    else:
        for line in output_rows(_iter(), headers, nl, arrays, json_cols):
            click.echo(line)

@cli.command()
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "--counts", help="Include row counts per view", default=False, is_flag=True
)
@output_options
@click.option(
    "--columns",
    help="Include list of columns for each view",
    is_flag=True,
    default=False,
)
@click.option(
    "--schema",
    help="Include schema for each view",
    is_flag=True,
    default=False,
)
@load_extension_option
def views(
        path,
        counts,
        nl,
        arrays,
        csv,
        tsv,
        no_headers,
        table,
        fmt,
        json_cols,
        columns,
        schema,
        load_extension,
):
    """List the views in the database

    Example:

    \b
        sqlite-utils views trees.db
    """
    tables.callback(
        path=path,
        fts4=False,
        fts5=False,
        counts=counts,
        nl=nl,
        arrays=arrays,
        csv=csv,
        tsv=tsv,
        no_headers=no_headers,
        table=table,
        fmt=fmt,
        json_cols=json_cols,
        columns=columns,
        schema=schema,
        load_extension=load_extension,
        views=True,
    )

@cli.command(name="create-table")
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("columns", nargs=-1, required=True)
@click.option("pks", "--pk", help="Column to use as primary key", multiple=True)
@click.option(
    "--not-null",
    multiple=True,
    help="Columns that should be created as NOT NULL",
)
@click.option(
    "--default",
    multiple=True,
    type=(str, str),
    help="Default value that should be set for a column",
)
@click.option(
    "--fk",
    multiple=True,
    type=(str, str, str),
    help="Column, other table, other column to set as a foreign key",
)
@click.option(
    "--ignore",
    is_flag=True,
    help="If table already exists, do nothing",
)
@click.option(
    "--replace",
    is_flag=True,
    help="If table already exists, replace it",
)
@click.option(
    "--transform",
    is_flag=True,
    help="If table already exists, try to transform the schema",
)
@load_extension_option
@click.option(
    "--strict",
    is_flag=True,
    help="Apply STRICT mode to created table",
)
def create_table(
        path,
        table,
        columns,
        pks,
        not_null,
        default,
        fk,
        ignore,
        replace,
        transform,
        load_extension,
        strict,
):
    """
    Add a table with the specified columns. Columns should be specified using
    name, type pairs, for example:

    \b
        sqlite-utils create-table my.db people \\
            id integer \\
            name text \\
            height float \\
            photo blob --pk id

    Valid column types are text, integer, float and blob.
    """
    db = duckdb_utils.Database(path)
    _load_extensions(db, load_extension)
    if len(columns) % 2 == 1:
        raise click.ClickException(
            "columns must be an even number of 'name' 'type' pairs"
        )
    coltypes = {}
    columns = list(columns)
    while columns:
        name = columns.pop(0)
        ctype = columns.pop(0)
        if ctype.upper() not in VALID_COLUMN_TYPES:
            raise click.ClickException(
                "column types must be one of {}".format(VALID_COLUMN_TYPES)
            )
        coltypes[name] = ctype.upper()
    # Does table already exist?
    if table in db.table_names():
        if not ignore and not replace and not transform:
            raise click.ClickException(
                'Table "{}" already exists. Use --replace to delete and replace it.'.format(
                    table
                )
            )
    db[table].create(
        coltypes,
        pk=pks[0] if len(pks) == 1 else pks,
        not_null=not_null,
        defaults=dict(default),
        foreign_keys=fk,
        ignore=ignore,
        replace=replace,
        transform=transform,
        strict=strict,
    )


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("sql")
@click.option(
    "--attach",
    type=(str, click.Path(file_okay=True, dir_okay=False, allow_dash=False)),
    multiple=True,
    help="Additional databases to attach - specify alias and filepath",
)
@output_options
@click.option("-r", "--raw", is_flag=True, help="Raw output, first column of first row")
@click.option("--raw-lines", is_flag=True, help="Raw output, first column of each row")
@click.option(
    "-p",
    "--param",
    multiple=True,
    type=(str, str),
    help="Named :parameters for SQL query",
)
@click.option(
    "--functions", help="Python code defining one or more custom SQL functions"
)
def query(path, sql):
    pass


_import_options = (
    click.option(
        "--flatten",
        is_flag=True,
        help='Flatten nested JSON objects, so {"a": {"b": 1}} becomes {"a_b": 1}',
    ),
    click.option("--nl", is_flag=True, help="Expect newline-delimited JSON"),
    click.option("-c", "--csv", is_flag=True, help="Expect CSV input"),
    click.option("--tsv", is_flag=True, help="Expect TSV input"),
    click.option("--empty-null", is_flag=True, help="Treat empty strings as NULL"),
    click.option(
        "--lines",
        is_flag=True,
        help="Treat each line as a single value called 'line'",
    ),
    click.option(
        "--text",
        is_flag=True,
        help="Treat input as a single value called 'text'",
    ),
    click.option("--convert", help="Python code to convert each item"),
    click.option(
        "--import",
        "imports",
        type=str,
        multiple=True,
        help="Python modules to import",
    ),
    click.option("--delimiter", help="Delimiter to use for CSV files"),
    click.option("--quotechar", help="Quote character to use for CSV/TSV"),
    click.option("--sniff", is_flag=True, help="Detect delimiter and quote character"),
    click.option("--no-headers", is_flag=True, help="CSV file has no header row"),
    click.option(
        "--encoding",
        help="Character encoding for input, defaults to utf-8",
    ),
)


def insert_upsert_options(*, require_pk=False):
    def inner(fn):
        for decorator in reversed(
                (
                        click.argument(
                            "path",
                            type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
                            required=True,
                        ),
                        click.argument("table"),
                        click.argument("file", type=click.File("rb"), required=True),
                        click.option(
                            "--pk",
                            help="Columns to use as the primary key, e.g. id",
                            multiple=True,
                            required=require_pk,
                        ),
                )
                + _import_options
                + (
                        click.option(
                            "--batch-size", type=int, default=100, help="Commit every X records"
                        ),
                        click.option("--stop-after", type=int, help="Stop after X records"),
                        click.option(
                            "--alter",
                            is_flag=True,
                            help="Alter existing table to add any missing columns",
                        ),
                        click.option(
                            "--not-null",
                            multiple=True,
                            help="Columns that should be created as NOT NULL",
                        ),
                        click.option(
                            "--default",
                            multiple=True,
                            type=(str, str),
                            help="Default value that should be set for a column",
                        ),
                        click.option(
                            "-d",
                            "--detect-types",
                            is_flag=True,
                            envvar="DUCKDB_UTILS_DETECT_TYPES",
                            help="Detect types for columns in CSV/TSV data",
                        ),
                        click.option(
                            "--analyze",
                            is_flag=True,
                            help="Run ANALYZE at the end of this operation",
                        ),
                        load_extension_option,
                        click.option("--silent", is_flag=True, help="Do not show progress bar"),
                        click.option(
                            "--strict",
                            is_flag=True,
                            default=False,
                            help="Apply STRICT mode to created table",
                        ),
                )
        ):
            fn = decorator(fn)
        return fn

    return inner


def insert_upsert_implementation(
        path,
        table,
        file,
        pk,
        flatten,
        nl,
        csv,
        tsv,
        empty_null,
        lines,
        text,
        convert,
        imports,
        delimiter,
        quotechar,
        sniff,
        no_headers,
        encoding,
        batch_size,
        stop_after,
        alter,
        upsert,
        ignore=False,
        replace=False,
        truncate=False,
        not_null=None,
        default=None,
        detect_types=None,
        analyze=False,
        load_extension=None,
        silent=False,
        bulk_sql=None,
        functions=None,
        strict=False,
):
    db = duckdb_utils.Database(path)
    _load_extensions(db, load_extension)
    if functions:
        # _register_functions(db, functions)
        pass
    if (delimiter or quotechar or sniff or no_headers) and not tsv:
        csv = True
    if (nl + csv + tsv) >= 2:
        raise click.ClickException("Use just one of --nl, --csv or --tsv")
    if (csv or tsv) and flatten:
        raise click.ClickException("--flatten cannot be used with --csv or --tsv")
    if empty_null and not (csv or tsv):
        raise click.ClickException("--empty-null can only be used with --csv or --tsv")
    if encoding and not (csv or tsv):
        raise click.ClickException("--encoding must be used with --csv or --tsv")
    if pk and len(pk) == 1:
        pk = pk[0]
    encoding = encoding or "utf-8-sig"

    # The --sniff option needs us to buffer the file to peek ahead
    sniff_buffer = None
    decoded_buffer = None
    if sniff:
        sniff_buffer = io.BufferedReader(file, buffer_size=4096)
        decoded_buffer = io.TextIOWrapper(sniff_buffer, encoding=encoding)
    else:
        decoded_buffer = io.TextIOWrapper(file, encoding=encoding)

    tracker = None
    with file_progress(decoded_buffer, silent=silent) as decoded:
        if csv or tsv:
            if sniff:
                # Read first 2048 bytes and use that to detect
                first_bytes = sniff_buffer.peek(2048)
                dialect = csv_std.Sniffer().sniff(
                    first_bytes.decode(encoding, "ignore")
                )
            else:
                dialect = "excel-tab" if tsv else "excel"
            csv_reader_args = {"dialect": dialect}
            if delimiter:
                csv_reader_args["delimiter"] = delimiter
            if quotechar:
                csv_reader_args["quotechar"] = quotechar
            reader = csv_std.reader(decoded, **csv_reader_args)
            first_row = next(reader)
            if no_headers:
                headers = ["untitled_{}".format(i + 1) for i in range(len(first_row))]
                reader = itertools.chain([first_row], reader)
            else:
                headers = first_row
            if empty_null:
                docs = (
                    dict(zip(headers, [None if cell == "" else cell for cell in row]))
                    for row in reader
                )
            else:
                docs = (dict(zip(headers, row)) for row in reader)
            if detect_types:
                tracker = TypeTracker()
                docs = tracker.wrap(docs)
        elif lines:
            docs = ({"line": line.strip()} for line in decoded)
        elif text:
            docs = ({"text": decoded.read()},)
        else:
            try:
                if nl:
                    docs = (json.loads(line) for line in decoded if line.strip())
                else:
                    docs = json.load(decoded)
                    if isinstance(docs, dict):
                        docs = [docs]
            except json.decoder.JSONDecodeError as ex:
                raise click.ClickException(
                    "Invalid JSON - use --csv for CSV or --tsv for TSV files\n\nJSON error: {}".format(
                        ex
                    )
                )
            if flatten:
                docs = (_flatten(doc) for doc in docs)

        if stop_after:
            docs = itertools.islice(docs, stop_after)

        if convert:
            variable = "row"
            if lines:
                variable = "line"
            elif text:
                variable = "text"
            fn = _compile_code(convert, imports, variable=variable)
            if lines:
                docs = (fn(doc["line"]) for doc in docs)
            elif text:
                # Special case: this is allowed to be an iterable
                text_value = list(docs)[0]["text"]
                fn_return = fn(text_value)
                if isinstance(fn_return, dict):
                    docs = [fn_return]
                else:
                    try:
                        docs = iter(fn_return)
                    except TypeError:
                        raise click.ClickException(
                            "--convert must return dict or iterator"
                        )
            else:
                docs = (fn(doc) or doc for doc in docs)

        extra_kwargs = {
            "ignore": ignore,
            "replace": replace,
            "truncate": truncate,
            "analyze": analyze,
            "strict": strict,
        }
        if not_null:
            extra_kwargs["not_null"] = set(not_null)
        if default:
            extra_kwargs["defaults"] = dict(default)
        if upsert:
            extra_kwargs["upsert"] = upsert

        # docs should all be dictionaries
        docs = (verify_is_dict(doc) for doc in docs)

        # Apply {"$base64": true, ...} decoding, if needed
        docs = (decode_base64_values(doc) for doc in docs)

        # For bulk_sql= we use cursor.executemany() instead
        if bulk_sql:
            if batch_size:
                doc_chunks = chunks(docs, batch_size)
            else:
                doc_chunks = [docs]
            for doc_chunk in doc_chunks:
                with db.conn:
                    db.conn.cursor().executemany(bulk_sql, doc_chunk)
            return

        try:
            db[table].insert_all(
                docs, pk=pk, batch_size=batch_size, alter=alter, **extra_kwargs
            )
        except Exception as e:
            if (
                    isinstance(e, OperationalError)
                    and e.args
                    and "has no column named" in e.args[0]
            ):
                raise click.ClickException(
                    "{}\n\nTry using --alter to add additional columns".format(
                        e.args[0]
                    )
                )
            # If we can find sql= and parameters= arguments, show those
            variables = _find_variables(e.__traceback__, ["sql", "parameters"])
            if "sql" in variables and "parameters" in variables:
                raise click.ClickException(
                    "{}\n\nsql = {}\nparameters = {}".format(
                        str(e), variables["sql"], variables["parameters"]
                    )
                )
            else:
                raise
        if tracker is not None:
            db[table].transform(types=tracker.types)

        # Clean up open file-like objects
        if sniff_buffer:
            sniff_buffer.close()
        if decoded_buffer:
            decoded_buffer.close()


@cli.command()
@insert_upsert_options()
@click.option(
    "--ignore", is_flag=True, default=False, help="Ignore records if pk already exists"
)
@click.option(
    "--replace",
    is_flag=True,
    default=False,
    help="Replace records if pk already exists",
)
@click.option(
    "--truncate",
    is_flag=True,
    default=False,
    help="Truncate table before inserting records, if table already exists",
)
def insert(
        path,
        table,
        file,
        pk,
        flatten,
        nl,
        csv,
        tsv,
        empty_null,
        lines,
        text,
        convert,
        imports,
        delimiter,
        quotechar,
        sniff,
        no_headers,
        encoding,
        batch_size,
        stop_after,
        alter,
        detect_types,
        analyze,
        load_extension,
        silent,
        ignore,
        replace,
        truncate,
        not_null,
        default,
        strict,
):
    """
    Insert records from FILE into a table, creating the table if it
    does not already exist.

    Example:

        echo '{"name": "Lila"}' | sqlite-utils insert data.db chickens -

    By default the input is expected to be a JSON object or array of objects.

    \b
    - Use --nl for newline-delimited JSON objects
    - Use --csv or --tsv for comma-separated or tab-separated input
    - Use --lines to write each incoming line to a column called "line"
    - Use --text to write the entire input to a column called "text"

    You can also use --convert to pass a fragment of Python code that will
    be used to convert each input.

    Your Python code will be passed a "row" variable representing the
    imported row, and can return a modified row.

    This example uses just the name, latitude and longitude columns from
    a CSV file, converting name to upper case and latitude and longitude
    to floating point numbers:

    \b
        sqlite-utils insert plants.db plants plants.csv --csv --convert '
          return {
            "name": row["name"].upper(),
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
          }'

    If you are using --lines your code will be passed a "line" variable,
    and for --text a "text" variable.

    When using --text your function can return an iterator of rows to
    insert. This example inserts one record per word in the input:

    \b
        echo 'A bunch of words' | sqlite-utils insert words.db words - \\
          --text --convert '({"word": w} for w in text.split())'
    """
    try:
        insert_upsert_implementation(
            path,
            table,
            file,
            pk,
            flatten,
            nl,
            csv,
            tsv,
            empty_null,
            lines,
            text,
            convert,
            imports,
            delimiter,
            quotechar,
            sniff,
            no_headers,
            encoding,
            batch_size,
            stop_after,
            alter=alter,
            upsert=False,
            ignore=ignore,
            replace=replace,
            truncate=truncate,
            detect_types=detect_types,
            analyze=analyze,
            load_extension=load_extension,
            silent=silent,
            not_null=not_null,
            default=default,
            strict=strict,
        )
    except UnicodeDecodeError as ex:
        raise click.ClickException(UNICODE_ERROR.format(ex))


def _load_extensions(db, load_extension):
    if load_extension:
        db.conn.enable_load_extension(True)
        for ext in load_extension:
            # if ext == "spatialite" and not os.path.exists(ext):
            #     ext = find_spatialite()
            if ":" in ext:
                path, _, entrypoint = ext.partition(":")
                db.conn.execute("SELECT load_extension(?, ?)", [path, entrypoint])
            else:
                db.conn.load_extension(ext)
