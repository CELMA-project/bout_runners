"""Global fixtures for the test routines."""


from distutils.dir_util import copy_tree
from distutils.dir_util import remove_tree
import shutil
from pathlib import Path
import pytest
from bout_runners.make.make import Make
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.utils.paths import get_bout_path
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.metadata.metadata_reader import MetadataReader



@pytest.fixture(scope='session', name='yield_bout_path')
def fixture_yield_bout_path():
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
def fixture_yield_conduction_path(yield_bout_path):
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


@pytest.fixture(scope='function')
def make_make_object(yield_bout_path):
    """
    Set up and tear down the make-object.

    In order not to make collisions with the global fixture which
    makes the `conduction` program, this fixture copies the content
    of the `conduction` directory to a `tmp` directory, which is
    removed in the teardown.

    This fixture calls make_obj.run_clean() before the yield statement.

    Parameters
    ----------
    yield_bout_path : Path
        Path to the BOUT++ repository. See the yield_bout_path fixture
        for more details

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    exec_file : Path
        The path to the executable

    See Also
    --------
    tests.bout_runners.conftest.get_bout_path : Fixture which returns
    the BOUT++ path
    """
    # Setup
    bout_path = yield_bout_path
    project_path = bout_path.joinpath('examples', 'conduction')
    tmp_path = project_path.parent.joinpath('tmp_make')

    copy_tree(str(project_path), str(tmp_path))

    exec_file = tmp_path.joinpath('conduction')

    make_obj = Make(makefile_root_path=tmp_path)
    make_obj.run_clean()

    yield make_obj, exec_file

    # Teardown
    remove_tree(str(tmp_path))


@pytest.fixture(scope='session')
def make_project(yield_conduction_path):
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield
    statement

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the yield_conduction_path for more details

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    project_path = yield_conduction_path

    make_obj = Make(makefile_root_path=project_path)
    make_obj.run_make()

    yield project_path

    # Teardown
    make_obj.run_clean()


@pytest.fixture(scope='session', name='get_test_data_path')
def fixture_get_test_data_path():
    """
    Return the test data path.

    Returns
    -------
    test_data_path : Path
        Path to the test data
    """
    return Path(__file__).absolute().parent.joinpath('data')


@pytest.fixture(scope='session', name='make_test_database')
def fixture_make_test_database():
    """
    Return the wrapped function for the database connection.

    Yields
    ------
    _make_db : function
        The function making the database
    """
    db_dir = Path(__file__).absolute().parent.joinpath('delme')
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


@pytest.fixture(scope='session', name='get_default_parameters')
def fixture_get_default_parameters(get_test_data_path):
    """
    Return the default parameters object.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Returns
    -------
    default_parameters : DefaultParameters
        The DefaultParameters object
    """
    settings_path = get_test_data_path.joinpath('BOUT.settings')
    default_parameters = \
        DefaultParameters(settings_path=settings_path)
    return default_parameters


@pytest.fixture(scope='session', name='make_test_schema')
def fixture_make_test_schema(get_default_parameters,
                             make_test_database):
    """
    Return the wrapped function for schema creation.

    Parameters
    ----------
    get_default_parameters : DefaultParameters
        The DefaultParameters object
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
        final_parameters_as_sql_types : dict
            Final parameters as sql types
        """
        db_connection = make_test_database(db_name)

        default_parameters = get_default_parameters
        final_parameters = FinalParameters(default_parameters)
        final_parameters_dict = final_parameters.get_final_parameters()
        final_parameters_as_sql_types = \
            final_parameters. \
            cast_parameters_to_sql_type(final_parameters_dict)

        db_creator = DatabaseCreator(db_connection)

        db_creator.\
            create_all_schema_tables(final_parameters_as_sql_types)

        return db_connection, final_parameters_as_sql_types

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
        Write to the split table.

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        db_connection : DatabaseConnector
            The database connection object
        """
        db_connection, _ = make_test_schema(db_name)

        db_writer = DatabaseWriter(db_connection)
        dummy_split_dict = {'number_of_processors': 1,
                            'number_of_nodes': 2,
                            'processors_per_node': 3}
        db_writer.create_entry('split', dummy_split_dict)

        return db_connection

    yield _write_split


@pytest.fixture(scope='session')
def copy_bout_inp():
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


@pytest.fixture(scope='function')
def yield_bout_path_conduction(yield_conduction_path):
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
        project_path = yield_conduction_path
        bout_paths = BoutPaths(project_path=project_path,
                               bout_inp_dst_dir=tmp_path_name)
        tmp_dir_list.append(bout_paths.bout_inp_dst_dir)

        return bout_paths

    yield _make_bout_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


@pytest.fixture(scope='function')
def copy_makefile(get_test_data_path):
    """
    Set up and tear down a copy of Makefile to my_makefile.

    Creates a temporary directory, copies Makefile from DATA_PATH to
    DATA_PATH/tmp/my_makefile to search for the Makefile.
    The file and directory are teared it down after the test.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    tmp_path : Path
        The path to the temporary directory
    """
    # Setup
    tmp_path = get_test_data_path.joinpath('tmp')
    tmp_path.mkdir(exist_ok=True)
    makefile_path = get_test_data_path.joinpath('Makefile')
    tmp_make = tmp_path.joinpath('my_makefile')
    shutil.copy(makefile_path, tmp_make)

    yield tmp_path

    # Teardown
    tmp_make.unlink()
    tmp_path.rmdir()


@pytest.fixture(scope='function')
def yield_number_of_rows_for_all_tables():
    """
    Yield the function used to count number of rows in a table.

    Yields
    ------
    _get_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    def _get_number_of_rows_for_all_tables(database_reader):
        """
        Return the number of rows for all tables in a schema.

        Parameters
        ----------
        database_reader : DatabaseReader
            The object used read from the database

        Returns
        -------
        number_of_rows_dict : dict
            Dict on the form
            >>> {'table_name': int}
        """
        number_of_rows_dict = dict()
        query_str = ("SELECT name FROM sqlite_master\n"
                     "    WHERE type ='table'\n"
                     "    AND name NOT LIKE 'sqlite_%'")
        table_of_tables = database_reader.query(query_str)
        for _, table_name_as_series in table_of_tables.iterrows():
            table_name = table_name_as_series['name']
            query_str = f'SELECT COUNT(*) AS rows FROM {table_name}'
            table = database_reader.query(query_str)
            number_of_rows_dict[table_name] = table.loc[0, 'rows']
        return number_of_rows_dict
    yield _get_number_of_rows_for_all_tables


@pytest.fixture(scope='session', name='yield_metadata_reader')
def fixture_yield_metadata_reader(get_test_data_path):
    """
    Yield the connection to the test database.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    MetadataReader
        The instance to read the metadata
    """
    test_db_connection =\
        DatabaseConnector(name='test',
                          database_root_path=get_test_data_path)
    yield MetadataReader(test_db_connection)


@pytest.fixture(scope='session', name='yield_all_table_names')
def fixture_yield_all_table_names(yield_metadata_reader):
    """
    Yield the table names from the test database.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The instance which reads the metadata

    Yields
    ------
    tuple
        A tuple of the tables in the test database
    """
    yield yield_metadata_reader.get_all_table_names()


@pytest.fixture(scope='session', name='yield_table_column_names')
def fixture_yield_table_column_names(yield_metadata_reader,
                                     yield_all_table_names):
    """
    Yield the table names from the test database.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The instance which reads the metadata
    yield_all_table_names : tuple
        A tuple of the tables in the test database

    Yields
    ------
    dict of tuple
        Dict containing the table and column names from the test
        database
    """
    yield yield_metadata_reader.get_all_column_names(
        yield_all_table_names)


@pytest.fixture(scope='session', name='yield_table_connections')
def fixture_yield_table_connections(yield_metadata_reader,
                                    yield_table_column_names):
    """
    Yield the table connection from the test database.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The instance which reads the metadata
    yield_table_column_names : dict
        Dict containing the table and column names from the test
        database

    Yields
    ------
    dict of tuple
        Dict containing the table connections from the test database
    """
    yield yield_metadata_reader.get_table_connection(
        yield_table_column_names)