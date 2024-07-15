import click
from click_default_group import DefaultGroup  # type: ignore

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


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
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("sql")
def query(path, sql):
    pass
