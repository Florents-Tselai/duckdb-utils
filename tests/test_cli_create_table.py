import json
import os
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from duckdb_utils import cli, Database


@pytest.mark.parametrize(
    "args,schema",
    [
        # No primary key
        (
                [
                    "name",
                    "text",
                    "age",
                    "integer",
                ],
                ('CREATE TABLE t("name" VARCHAR, age INTEGER);'),
        ),
        # All types:
        (
                [
                    "id",
                    "integer",
                    "name",
                    "text",
                    "age",
                    "integer",
                    "weight",
                    "float",
                    "thumbnail",
                    "blob",
                    "--pk",
                    "id",
                ],
                (
                        'CREATE TABLE t(id INTEGER PRIMARY KEY, "name" VARCHAR, age INTEGER, weight '
                        'FLOAT, thumbnail BLOB);'
                ),
        ),
        # Not null:
        (
                ["name", "text", "--not-null", "name"],
                'CREATE TABLE t("name" VARCHAR NOT NULL);',
        ),
        # Default:
        (
                ["age", "integer", "--default", "age", "3"],
                ("CREATE TABLE t(age INTEGER DEFAULT('3'));"),
        ),
        # Compound primary key
        (
                ["category", "text", "name", "text", "--pk", "category", "--pk", "name"],
                (
                        'CREATE TABLE t(category VARCHAR, "name" VARCHAR, PRIMARY KEY(category, "name"));'
                ),
        ),
    ],
)
def test_create_table(args, schema):
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli.cli,
            [
                "create-table",
                "test.db",
                "t",
            ]
            + args,
            catch_exceptions=False,
            )
        assert result.exit_code == 0
        db = Database("test.db")
        assert schema == db["t"].schema


def test_create_table_foreign_key():
    runner = CliRunner()
    creates = (
        ["authors", "id", "integer", "name", "text", "--pk", "id"],
        [
            "books",
            "id",
            "integer",
            "title",
            "text",
            "author_id",
            "integer",
            "--pk",
            "id",
            "--fk",
            "author_id",
            "authors",
            "id",
        ],
    )
    with runner.isolated_filesystem():
        for args in creates:
            result = runner.invoke(
                cli.cli, ["create-table", "books.db"] + args, catch_exceptions=False
            )
            assert result.exit_code == 0
        db = Database("books.db")
        assert 'CREATE TABLE authors(id INTEGER PRIMARY KEY, "name" VARCHAR);' == db["authors"].schema
        assert 'CREATE TABLE books(id INTEGER PRIMARY KEY, title VARCHAR, author_id INTEGER, FOREIGN KEY (author_id) REFERENCES authors(id));' == db["books"].schema

# def test_create_table_ignore():
#     runner = CliRunner()
#     with runner.isolated_filesystem():
#         db = Database("test.db")
#         db["dogs"].insert({"name": "Cleo"})
#         result = runner.invoke(
#             cli.cli, ["create-table", "test.db", "dogs", "id", "integer", "--ignore"]
#         )
#         assert result.exit_code == 0
#         assert "CREATE TABLE [dogs] (\n   [name] TEXT\n)" == db["dogs"].schema
#
#
# def test_create_table_replace():
#     runner = CliRunner()
#     with runner.isolated_filesystem():
#         db = Database("test.db")
#         db["dogs"].insert({"name": "Cleo"})
#         result = runner.invoke(
#             cli.cli, ["create-table", "test.db", "dogs", "id", "integer", "--replace"]
#         )
#         assert result.exit_code == 0
#         assert "CREATE TABLE [dogs] (\n   [id] INTEGER\n)" == db["dogs"].schema
#
#

# @pytest.mark.parametrize("strict", (False, True))
# def test_create_table_strict(strict):
#     runner = CliRunner()
#     with runner.isolated_filesystem():
#         db = Database("test.db")
#         result = runner.invoke(
#             cli.cli,
#             ["create-table", "test.db", "items", "id", "integer"]
#             + (["--strict"] if strict else []),
#             )
#         assert result.exit_code == 0
#         assert db["items"].strict == strict or not db.supports_strict
#
#