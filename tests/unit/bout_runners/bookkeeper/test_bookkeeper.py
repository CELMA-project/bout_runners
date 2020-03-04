"""Contains unittests for the bookkeeper."""


def test_create_table(make_test_database):
    """
    Test query and create_single_table.

    Parameters
    ----------
    make_test_database : function
        The bookkeeper from the fixture
    """
    bookkeeper = make_test_database('create_single_table.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bookkeeper.query(query_str)

    # Should be empty
    assert len(table.index) == 0

    bookkeeper.create_single_table('CREATE TABLE foo (bar, TEXT)')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bookkeeper.query(query_str)

    # Should contain exactly 1
    assert len(table.index) == 1


def test_create_parameter_tables(make_test_database):
    """
    Test create_parameter_tables.

    Parameters
    ----------
    make_test_database : function
        The bookkeeper from the fixture
    """
    bookkeeper = make_test_database('create_parameter_table.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bookkeeper.query(query_str)

    # Should be empty
    assert len(table.index) == 0

    parameter_dict = dict(foo=dict(bar='INTEGER'),
                          baz=dict(foobar='INTEGER'))

    bookkeeper.create_parameter_tables(parameter_dict)

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bookkeeper.query(query_str)

    # Should contain foo, baz and parameters
    assert len(table.index) == 3
