"""Contains unittests for the database creator."""


import pytest
import sqlite3
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_creator import DatabaseCreator


def test_database_creator(get_test_data_path, make_test_database):
    """
    Test we can create the database schemas.

    Specifically this test that:
    1. The database is empty on creation
    2. The tables are created
    3. It is not possible to create the schema more than once

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    make_test_database : function
        Function returning the database connection
    """
    db_connection = make_test_database('creation_test')
    db_reader = DatabaseReader(db_connection)

    # There should be no tables before creating them
    assert not db_reader.check_tables_created()

    settings_path = get_test_data_path.joinpath('BOUT.settings')

    # Obtain the final_parameters
    default_parameters = DefaultParameters(settings_path=settings_path)
    final_parameters = FinalParameters(default_parameters)
    final_parameters_dict = final_parameters.get_final_parameters()
    final_parameters_as_sql_types = \
        final_parameters.\
        cast_parameters_to_sql_type(final_parameters_dict)

    db_creator = DatabaseCreator(db_connection)

    db_creator.create_all_schema_tables(final_parameters_as_sql_types)

    # The tables should now have been created
    assert db_reader.check_tables_created()

    with pytest.raises(sqlite3.OperationalError):
        db_creator.\
            create_all_schema_tables(final_parameters_as_sql_types)
