from duckdb_utils import Database
from sqlite_utils.utils import sqlite3
import pytest
import duckdb

CREATE_TABLES = """
create table Gosh (c1 text, c2 text, c3 text);
create table Gosh2 (c1 text, c2 text, c3 text);
"""


def pytest_configure(config):
    import sys

    sys._called_from_test = True


@pytest.fixture
def fresh_db():
    return Database(memory=True)


@pytest.fixture
def existing_db():
    database = Database(memory=True)
    database.execute(
        """
        CREATE TABLE foo (text TEXT);
        INSERT INTO foo (text) values ('one');
        INSERT INTO foo (text) values ('two');
        INSERT INTO foo (text) values ('three');
        
        CREATE TABLE bar (c1 TEXT, c2 INTEGER);
        INSERT INTO bar (c1, c2) values ('c0', 0);
        INSERT INTO bar (c1, c2) values ('c1', 1);
        INSERT INTO bar (c1, c2) values ('c2', 2);
    """
    )
    return database


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = duckdb.connect(path)
    db.execute(CREATE_TABLES)
    return path
