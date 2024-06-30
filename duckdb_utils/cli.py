import click
@click.group()
@click.version_option()
def cli():
    """ CLI for duckdb_utils """
    pass

@cli.command()
@click.argument(
    "name",
    type=str,
    required=True,
)
def hello(name):
    print(f"Hello, {name}")