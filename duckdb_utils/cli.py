import click
import tabulate
from click_default_group import DefaultGroup  # type: ignore
from sqlite_utils.cli import (output_rows, VALID_COLUMN_TYPES)
import csv as csv_std
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
                            envvar="SQLITE_UTILS_DETECT_TYPES",
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
    pass


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
