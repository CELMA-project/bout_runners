"""Contains unittests for the database creator."""


import pytest
import sqlite3
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_creator import DatabaseCreator


def test_database_creator(make_test_database, make_test_schema):
    """
    Test we can create the database schemas.

    Specifically this test that:
    1. The database is empty on creation
    2. The tables are created
    3. It is not possible to create the schema more than once

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
