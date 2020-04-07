"""Contains unittests for the database creator."""


import pytest
import sqlite3
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_creator import DatabaseCreator


def test_database_creator(make_test_database, make_test_schema):
    """
    Test we can create the database schemas.

    Specifically this test that:
    1. The database is empty on creation
    2. The tables are created
    3. It is not possible to create the schema more than once
    4. Check that all expected tables have been created

    Parameters
    ----------
    make_test_database : function
        Function returning the database connection
    make_test_schema : function
        Function returning the database connection and the final
        parameters as sql types
    """
    db_connection_no_schema = \
        make_test_database('test_creation_without_schema')
    db_reader_no_schema = DatabaseReader(db_connection_no_schema)

    # There should be no tables before creating them
    assert not db_reader_no_schema.check_tables_created()

    db_connection_schema, final_parameters_as_sql_types = \
        make_test_schema('test_creation_with_schema')
    db_reader_schema = DatabaseReader(db_connection_schema)
    db_creator = DatabaseCreator(db_connection_schema)

    # The tables should now have been created
    assert db_reader_schema.check_tables_created()

    with pytest.raises(sqlite3.OperationalError):
        db_creator.\
            create_all_schema_tables(final_parameters_as_sql_types)

    # Check that all tables has been created
    non_parameter_tables = {'system_info',
                            'split',
                            'file_modification',
                            'parameters',
                            'run'}
    parameter_tables = set(el.replace(':', '_') for
                           el in final_parameters_as_sql_types.keys())
    query_str = ('SELECT name FROM sqlite_master\n'
                 '   WHERE type="table"')
    table = db_reader_schema.query(query_str)

    assert non_parameter_tables.union(parameter_tables) == \
        set(table.loc[:, 'name'].values)


def test_get_create_table_statement():
    """Test that get_create_table_statement returns expected output."""
    result = DatabaseCreator.get_create_table_statement(
        table_name='foo',
        columns=dict(bar='baz',
                     foobar='qux'),
        primary_key='quux',
        foreign_keys=dict(quuz=('corge', 'grault'),
                          garply=('waldo', 'fred')))

    expected = ('CREATE TABLE foo \n'
                '(   quux INTEGER PRIMARY KEY,\n'
                '    bar baz NOT NULL,\n'
                '    foobar qux NOT NULL,\n'
                '    quuz INTEGER NOT NULL,\n'
                '    garply INTEGER NOT NULL,\n'
                '    FOREIGN KEY(quuz) \n'
                '        REFERENCES corge(grault)\n'
                '            ON UPDATE CASCADE\n'
                '            ON DELETE CASCADE,\n'
                '    FOREIGN KEY(garply) \n'
                '        REFERENCES waldo(fred)\n'
                '            ON UPDATE CASCADE\n'
                '            ON DELETE CASCADE)')

    assert result == expected
