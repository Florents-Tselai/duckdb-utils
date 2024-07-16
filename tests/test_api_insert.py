def test_api_existing_rows_where(existing_db):
    foo = existing_db.table('bar')
    assert list(foo.rows_where()) == [{'c1': 'c0', 'c2': 0},
                                      {'c1': 'c1', 'c2': 1},
                                      {'c1': 'c2', 'c2': 2}]
