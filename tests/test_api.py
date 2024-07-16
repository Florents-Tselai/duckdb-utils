"""
Mos sqlite-utils tests are CLI-oriented and the API
has been created after the CLI.
This has proven tricky to port.

Hence, while fleshing out the API,
tests are added here as they are necessary
"""

import types


def test_api_queryable_table(existing_db):
    bar = existing_db.table('bar')
    assert (list(bar.rows_where()) ==
            list(bar.rows) ==
            [{'c1': 'c0', 'c2': 0},
             {'c1': 'c1', 'c2': 1},
             {'c1': 'c2', 'c2': 2}])

    # assert bar.count_where() == bar.count == 3


def test_existing_query(existing_db):
    results = existing_db.query("select * from foo")
    assert isinstance(results, types.GeneratorType)
    assert list(results) == [{'text': 'one'}, {'text': 'two'}, {'text': 'three'}]


def test_existing_execute(existing_db):
    results = existing_db.execute("select * from foo").fetchall()
    assert list(results) == [('one',), ('two',), ('three',)]

# def test_existing_execute_returning_dicts(existing_db):
#     # Like db.query() but returns a list, included for backwards compatibility
#     # see https://github.com/simonw/sqlite-utils/issues/290
#     assert existing_db.execute_returning_dicts("select * from foo") == [{'text': 'one'}, {'text': 'two'}, {'text': 'three'}]


# def test_query(fresh_db):
#     fresh_db["dogs"].insert_all([{"name": "Cleo"}, {"name": "Pancakes"}])
#     results = fresh_db.query("select * from dogs order by name desc")
#     assert isinstance(results, types.GeneratorType)
#     assert list(results) == [{"name": "Pancakes"}, {"name": "Cleo"}]
#
#
# def test_execute_returning_dicts(fresh_db):
#     # Like db.query() but returns a list, included for backwards compatibility
#     # see https://github.com/simonw/sqlite-utils/issues/290
#     fresh_db["test"].insert({"id": 1, "bar": 2}, pk="id")
#     assert fresh_db.execute_returning_dicts("select * from test") == [
#         {"id": 1, "bar": 2}
#     ]
