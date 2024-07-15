import click
from click_default_group import DefaultGroup  # type: ignore


@click.group(cls=DefaultGroup, default="query", default_if_no_args=True)
@click.version_option()
def cli():
    "Commands for interacting with a SQLite database"
    pass
