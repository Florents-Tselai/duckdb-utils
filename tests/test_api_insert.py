"""
Mos sqlite-utils tests are CLI-oriented and the API
has been created after the CLI.
This has proven tricky to port.

Hence, while fleshing out the API,
tests are added here as they are necessary
"""
def test_api_existing_rows_where(existing_db):
    foo = existing_db.table('bar')
    assert list(foo.rows_where()) == [{'c1': 'c0', 'c2': 0},
                                      {'c1': 'c1', 'c2': 1},
                                      {'c1': 'c2', 'c2': 2}]
