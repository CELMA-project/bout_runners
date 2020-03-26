"""Global fixtures for the test routines."""


import shutil
from pathlib import Path
import pytest
from bout_runners.make.make import MakeProject
from bout_runners.runner.runner import BoutRunner
from bout_runners.utils.paths import get_bout_path
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter


@pytest.fixture(scope='session', name='yield_bout_path')
def fixture_get_bout_path():
    """
    Load the dot-env file and yield the bout_path.

    Yields
    ------
    bout_path : Path
        Path to the BOUT++ repository
    """
    bout_path = get_bout_path()

    yield bout_path


@pytest.fixture(scope='session', name='yield_conduction_path')
def fixture_get_conduction_path(yield_bout_path):
    """
    Yield the conduction path.

    Yields
    ------
    conduction_path : Path
        Path to the BOUT++ conduction example
    """
    bout_path = yield_bout_path
    conduction_path = bout_path.joinpath('examples', 'conduction')

    yield conduction_path


@pytest.fixture(scope='session', name='make_project')
def fixture_make_project(yield_conduction_path):
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield
    statement

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the fixture_get_conduction_path for more details

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    project_path = yield_conduction_path

    make_obj = MakeProject(makefile_root_path=project_path)
    make_obj.run_make()

    yield project_path

    # Teardown
    make_obj.run_clean()


@pytest.fixture(scope='session')
def get_test_data_path():
    """
    Return the test data path.

    Returns
    -------
    test_data_path : Path
        Path to the test data
    """
    return Path(__file__).absolute().parent.joinpath('data')


@pytest.fixture(scope='session')
def make_test_database():
    """
    Return the wrapped function for the database connection.

    Yields
    ------
    _make_db : function
        The function making the database
    """
    db_dir = Path(__file__).absolute().parents[2].joinpath('delme')
    db_dir.mkdir(exist_ok=True, parents=True)

    def _make_db(db_name=None):
        """
        Make a database.

        It makes sense to have one database per test as we are
        testing the content of the database

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        DatabaseConnector
            The database connection object
        """
        return DatabaseConnector(name=db_name,
                                 database_root_path=db_dir)

    yield _make_db

    shutil.rmtree(db_dir)


@pytest.fixture(scope='session')
def make_test_schema(get_test_data_path, make_test_database):
    """
    Return the wrapped function for schema creation.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    make_test_database : function
        Function returning the database connection

    Yields
    ------
    _make_schema : function
        The function making the schema (i.e. making all the tables)
    """
    def _make_schema(db_name=None):
        """
        Create the schema (i.e. make all the tables) of the database.

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        db_connection : DatabaseConnector
            The database connection object
        """
        db_connection = make_test_database(db_name)

        settings_path = get_test_data_path.joinpath('BOUT.settings')

        run_parameters_dict = \
            BoutRunner.obtain_project_parameters(settings_path)
        parameters_as_sql_types = \
            BoutRunner.cast_parameters_to_sql_type(run_parameters_dict)

        db_creator = DatabaseCreator(db_connection)

        db_creator.create_all_schema_tables(parameters_as_sql_types)

        return db_connection

    yield _make_schema


@pytest.fixture(scope='session')
def write_to_split(make_test_schema):
    """
    Return the wrapped function for writing to the split table.

    Parameters
    ----------
    make_test_schema : function
        Function returning the database connection with the schema
        created

    Yields
    ------
    _write_split : function
        The function writing to the split table
    """
    def _write_split(db_name=None):
        """
        writing to the split table.

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        db_connection : DatabaseConnector
            The database connection object
        """
        db_connection = make_test_schema(db_name)

        db_writer = DatabaseWriter(db_connection)
        dummy_split_dict = {'number_of_processors': 1,
                            'nodes': 2,
                            'processors_per_node': 3}
        db_writer.create_entry('split', dummy_split_dict)

        return db_connection

    yield _write_split
