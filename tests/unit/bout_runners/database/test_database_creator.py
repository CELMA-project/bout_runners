"""Contains unittests for the database creator."""


import pytest
import sqlite3
from bout_runners.runner.runner import BoutRunner
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_creator import DatabaseCreator


def test_database_creator(make_test_database, get_test_data_path):
    """
    Test we can create the database schemas.

    Specifically this test that:
    1. The database is empty on creation
    2. The tables are created
    3. It is not possible to create the schema more than once

    Parameters
    ----------
    make_test_database : function
        The database from the fixture
    get_test_data_path : Path
        Path to the test data
    """

    db_connection = make_test_database('creation_test')
    db_reader = DatabaseReader(db_connection)

    # There should be no tables before creating them
    assert not db_reader.check_tables_created()

    settings_path = get_test_data_path.joinpath('BOUT.settings')

    run_parameters_dict = \
        BoutRunner.obtain_project_parameters(settings_path)
    parameters_as_sql_types = \
        BoutRunner.cast_parameters_to_sql_type(run_parameters_dict)

    db_creator = DatabaseCreator(db_connection)

    db_creator.create_all_schema_tables(parameters_as_sql_types)

    # The tables should now have been created
    assert db_reader.check_tables_created()

    with pytest.raises(sqlite3.OperationalError):
        db_creator.create_all_schema_tables(parameters_as_sql_types)
