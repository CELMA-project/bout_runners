"""Global fixtures for the test routines."""


import shutil
from pathlib import Path
import pytest
from bout_runners.make.make import MakeProject
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.utils.paths import get_bout_path
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.executor.bout_paths import BoutPaths


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


@pytest.fixture(scope='session', name='get_conduction_path')
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
def fixture_make_project(get_conduction_path):
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield
    statement

    Parameters
    ----------
    get_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the fixture_get_conduction_path for more details

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    project_path = get_conduction_path

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

        default_parameters = DefaultParameters(
            settings_path=settings_path)
        final_parameters = FinalParameters(default_parameters)
        final_parameters_dict = final_parameters.get_final_parameters()
        final_parameters_as_sql_types = \
            final_parameters. \
            cast_parameters_to_sql_type(final_parameters_dict)

        db_creator = DatabaseCreator(db_connection)

        db_creator.\
            create_all_schema_tables(final_parameters_as_sql_types)

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


@pytest.fixture(scope='session', name='copy_bout_inp')
def fixture_copy_bout_inp():
    """
    Copy BOUT.inp to a temporary directory.

    Yields
    ------
    _copy_inp_path : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _copy_inp_path(project_path, tmp_path_name):
        """
        Copy BOUT.inp to a temporary directory.

        Parameters
        ----------
        project_path : Path
            Root path to the project
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        tmp_bout_inp_dir : Path
            Path to the temporary directory
        """
        bout_inp_path = project_path.joinpath('data', 'BOUT.inp')

        tmp_bout_inp_dir = project_path.joinpath(tmp_path_name)
        tmp_bout_inp_dir.mkdir(exist_ok=True)
        tmp_dir_list.append(tmp_bout_inp_dir)

        shutil.copy(bout_inp_path,
                    tmp_bout_inp_dir.joinpath('BOUT.inp'))

        return tmp_bout_inp_dir

    yield _copy_inp_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


@pytest.fixture(scope='function', name='get_bout_path_conduction')
def fixture_get_bout_path_conduction(get_conduction_path):
    """
    Make the bout_path object and clean up after use.

    Yields
    ------
    _make_bout_path : function
        Function which makes the BoutPaths object for the conduction
        example
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _make_bout_path(tmp_path_name):
        """
        Create BoutPaths from the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        bout_paths : BoutPaths
            The BoutPaths object
        """
        project_path = get_conduction_path
        bout_paths = BoutPaths(project_path=project_path,
                               bout_inp_dst_dir=tmp_path_name)
        tmp_dir_list.append(bout_paths.bout_inp_dst_dir)

        return bout_paths

    yield _make_bout_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)
