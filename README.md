# duckdb-utils: CLI tool and Python library for manipulating DuckDB databases

[![Github](https://img.shields.io/static/v1?label=GitHub&message=Repo&logo=GitHub&color=green)](https://github.com/Florents-Tselai/duckdb-utils)
[![PyPI](https://img.shields.io/pypi/v/duckdb-utils.svg)](https://pypi.org/project/duckdb-utils/)
[![Documentation Status](https://readthedocs.org/projects/duckdb-utils/badge/?version=stable)](http://duckdb-utils.tselai.com/en/latest/?badge=stable)
[![Linkedin](https://img.shields.io/badge/LinkedIn-0077B5?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/florentstselai/)
[![Github Sponsors](https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=pink)](https://github.com/sponsors/Florents-Tselai/)
[![pip installs](https://img.shields.io/pypi/dm/duckdb-utils?label=pip%20installs)](https://pypi.org/project/duckdb-utils/)
[![Tests](https://github.com/Florents-Tselai/duckdb-utils/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/Florents-Tselai/duckdb-utils/actions?query=workflow%3ATest)
[![codecov](https://codecov.io/gh/Florents-Tselai/duckdb-utils/branch/main/graph/badge.svg)](https://codecov.io/gh/Florents-Tselai/duckdb-utils)
[![License](https://img.shields.io/badge/BSD%20license-blue.svg)](https://github.com/Florents-Tselai/duckdb-utils/blob/main/LICENSE)

CLI tool and Python library for manipulating DuckDB databases

**Inspired by and based on [sqlite-utils](https://github.com/simonw/sqlite-utils)**.
Standard DuckDB ships with more powerful batteries than SQLite does,
which may yield some of of sqlite-utils CLI offerings unnecessary.
The Python API however that sqlite-utils exposes it's really well-designed 
and pythonic.

What's worthy of porting and what's not, will be decieded on
a per-case basis.

## API


## CLI

```shell
Usage: duckdb-utils [OPTIONS] COMMAND [ARGS]...

  Commands for interacting with a DuckDB database

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  query*
  create-table  Add a table with the specified columns.
  insert        Insert records from FILE into a table, creating the table...
  tables        List the tables in the database
  views         List the views in the database
```

### ```duckdb-utils create-table```

```shell
Usage: duckdb-utils create-table [OPTIONS] PATH TABLE COLUMNS...

  Add a table with the specified columns. Columns should be specified using
  name, type pairs, for example:

      duckdb-utils create-table my.db people \
          id integer \
          name text \
          height float \
          photo blob --pk id

  Valid column types are text, integer, float and blob.

Options:
  --pk TEXT                 Column to use as primary key
  --not-null TEXT           Columns that should be created as NOT NULL
  --default <TEXT TEXT>...  Default value that should be set for a column
  --fk <TEXT TEXT TEXT>...  Column, other table, other column to set as a
                            foreign key
  --ignore                  If table already exists, do nothing
  --replace                 If table already exists, replace it
  --transform               If table already exists, try to transform the
                            schema
  --load-extension TEXT     Path to SQLite extension, with optional
                            :entrypoint
  --strict                  Apply STRICT mode to created table
  -h, --help                Show this message and exit.
```

