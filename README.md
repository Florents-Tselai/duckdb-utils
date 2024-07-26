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
which may make some of the sqlite-utils CLI offerings unnecessary.
The Python API, however that sqlite-utils exposes it's really well-designed 
and pythonic.

What's worthy of porting and what's not, will be decieded on
a per-case basis.

**NOTE**: Initially, I tried (very hard) to avoid a complete lift-and-shift
of the sqlite-utils code and use inheritance and monkey-patching instead,
but it's proved trickier than I had hoped. Hence, I had to lift-and-shift some core abstractions (see https://github.com/Florents-Tselai/duckdb-utils/pull/14).

## API

```python
from duckdb_utils import Database

db = Database(memory=True)
db.execute(
  """
  CREATE TABLE bar (c1 TEXT, c2 INTEGER);
  INSERT INTO bar (c1, c2) values ('c0', 0);
  INSERT INTO bar (c1, c2) values ('c1', 1);
  INSERT INTO bar (c1, c2) values ('c2', 2);
  """
)

 bar = db.table('bar')

assert bar.exists()
assert not existing_db.table('gsdfgf').exists()

assert bar.count_where() == bar.count == 3

assert (list(bar.rows_where()) ==
        list(bar.rows) ==
        [{'c1': 'c0', 'c2': 0},
         {'c1': 'c1', 'c2': 1},
         {'c1': 'c2', 'c2': 2}])

assert bar.columns == [Column(cid=0, name='c1', type='VARCHAR', notnull=False, default_value=None, is_pk=False),
                       Column(cid=1, name='c2', type='INTEGER', notnull=False, default_value=None, is_pk=False)]

assert bar.columns_dict == {'c1': str, 'c2': int}

assert bar.schema == 'CREATE TABLE bar(c1 VARCHAR, c2 INTEGER);'

assert list(bar.pks_and_rows_where()) == [(0, {'c1': 'c0', 'c2': 0, 'rowid': 0}),
                                          (1, {'c1': 'c1', 'c2': 1, 'rowid': 1}),
                                          (2, {'c1': 'c2', 'c2': 2, 'rowid': 2})]

assert list(db.query("select * from bar")) == [{'c1': 'c0', 'c2': 0}, {'c1': 'c1', 'c2': 1}, {'c1': 'c2', 'c2': 2}]


assert list(db.execute("select * from bar").fetchall()) == [('c0', 0), ('c1', 1), ('c2', 2)]

```


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
