# pylint: disable=c0302
"""Global fixtures for the test routines."""


import shutil
from distutils.dir_util import copy_tree, remove_tree
from pathlib import Path
from typing import Callable, Dict, Iterator, Tuple, Optional

import pandas as pd
import psutil
import pytest
from pandas import DataFrame
from _pytest.monkeypatch import MonkeyPatch
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.make.make import Make
from bout_runners.metadata.metadata_reader import MetadataReader
from bout_runners.metadata.metadata_updater import MetadataUpdater
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.utils.paths import get_bout_directory, get_config_path
from bout_runners.executor.executor import Executor
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_run_setup import BoutRunSetup


@pytest.fixture(scope="session", name="yield_bout_path")
def fixture_yield_bout_path() -> Iterator[Path]:
    """
    Yield the BOUT++ path.

    Yields
    ------
    bout_path : Path
        Path to the BOUT++ repository
    """
    bout_path = get_bout_directory()

    yield bout_path


@pytest.fixture(scope="session", name="yield_conduction_path")
def fixture_yield_conduction_path(yield_bout_path: Path) -> Iterator[Path]:
    """
    Yield the conduction path.

    Parameters
    ----------
    yield_bout_path : Path
        Path to BOUT++

    Yields
    ------
    conduction_path : Path
        Path to the BOUT++ conduction example
    """
    bout_path = yield_bout_path
    conduction_path = bout_path.joinpath("examples", "conduction")

    yield conduction_path


@pytest.fixture(scope="function")
def make_make_object(yield_bout_path: Path) -> Iterator[Tuple[Make, Path]]:
    """
    Set up and tear down the make-object.

    In order not to make collisions with the global fixture which makes the
    `conduction` program, this fixture copies the content of the `conduction`
    directory to a `tmp` directory, which is removed in the teardown.

    This fixture calls make_obj.run_clean() before the yield statement.

    Parameters
    ----------
    yield_bout_path : Path
        Path to the BOUT++ repository. See the yield_bout_path fixture for more details

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    exec_file : Path
        The path to the executable

    See Also
    --------
    tests.bout_runners.conftest.get_bout_directory : Fixture returning the BOUT++ path
    """
    # Setup
    bout_path = yield_bout_path
    project_path = bout_path.joinpath("examples", "conduction")
    tmp_path = project_path.parent.joinpath("tmp_make")

    copy_tree(str(project_path), str(tmp_path))

    exec_file = tmp_path.joinpath("conduction")

    make_obj = Make(makefile_root_path=tmp_path)
    make_obj.run_clean()

    yield make_obj, exec_file

    # Teardown
    remove_tree(str(tmp_path))


@pytest.fixture(scope="session")
def make_project(yield_conduction_path: Path) -> Iterator[Path]:
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield statement

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example
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


@pytest.fixture(scope="session", name="get_test_data_path")
def fixture_get_test_data_path() -> Path:
    """
    Return the test data path.

    Returns
    -------
    test_data_path : Path
        Path to the test data
    """
    return Path(__file__).absolute().parent.joinpath("data")


@pytest.fixture(scope="session", name="get_tmp_db_dir")
def fixture_get_tmp_db_dir() -> Iterator[Path]:
    """
    Return the directory for the temporary databases.

    Yields
    ------
    tmp_db_dir : Path
        Path to the temporary database directory
    """
    tmp_db_dir = Path(__file__).absolute().parent.joinpath("delme")
    tmp_db_dir.mkdir(exist_ok=True, parents=True)
    yield tmp_db_dir

    shutil.rmtree(tmp_db_dir)


@pytest.fixture(scope="session", name="make_test_database")
def fixture_make_test_database(
    get_tmp_db_dir: Path,
) -> Callable[[Optional[str]], DatabaseConnector]:
    """
    Return the wrapped function for the database connection.

    Parameters
    ----------
    get_tmp_db_dir: Path
        The directory for the temporary databases

    Returns
    -------
    _make_db : function
        Function making an empty database
    """

    def _make_db(db_name: Optional[str] = None) -> DatabaseConnector:
        """
        Make a database.

        It makes sense to have one database per test as we are testing the content of
        the database

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        DatabaseConnector
            The database connection object
        """
        return DatabaseConnector(name=db_name, db_root_path=get_tmp_db_dir)

    return _make_db


@pytest.fixture(scope="session", name="get_default_parameters")
def fixture_get_default_parameters(get_test_data_path: Path) -> DefaultParameters:
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
    settings_path = get_test_data_path.joinpath("BOUT.settings")
    default_parameters = DefaultParameters(settings_path=settings_path)
    return default_parameters


@pytest.fixture(scope="session", name="make_test_schema")
def fixture_make_test_schema(
    get_default_parameters: DefaultParameters,
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
) -> Iterator[
    Callable[[Optional[str]], Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]]
]:
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

    def _make_schema(
        db_name: Optional[str] = None,
    ) -> Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]:
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
        final_parameters_as_sql_types = final_parameters.cast_to_sql_type(
            final_parameters_dict
        )

        db_creator = DatabaseCreator(db_connection)

        db_creator.create_all_schema_tables(final_parameters_as_sql_types)

        return db_connection, final_parameters_as_sql_types

    yield _make_schema


@pytest.fixture(scope="session")
def write_to_split(
    make_test_schema: Callable[
        [Optional[str]], Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]
    ],
) -> Iterator[Callable[[Optional[str]], DatabaseConnector]]:
    """
    Return the wrapped function for writing to the split table.

    Parameters
    ----------
    make_test_schema : function
        Function returning the database connection with the schema created

    Yields
    ------
    _write_split : function
        The function writing to the split table
    """

    def _write_split(db_name: Optional[str] = None) -> DatabaseConnector:
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
        dummy_split_dict = {
            "number_of_processors": 1,
            "number_of_nodes": 2,
            "processors_per_node": 3,
        }
        db_writer.create_entry("split", dummy_split_dict)

        return db_connection

    yield _write_split


@pytest.fixture(scope="session")
def copy_bout_inp() -> Iterator[Callable[[Path, str], Path]]:
    """
    Copy BOUT.inp to a temporary directory.

    Yields
    ------
    _copy_inp_path : function
        Function which copies BOUT.inp and returns the path to the temporary directory
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _copy_inp_path(project_path: Path, tmp_path_name: str) -> Path:
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
        bout_inp_path = project_path.joinpath("data", "BOUT.inp")

        tmp_bout_inp_dir = project_path.joinpath(tmp_path_name)
        tmp_bout_inp_dir.mkdir(exist_ok=True)
        tmp_dir_list.append(tmp_bout_inp_dir)

        shutil.copy(bout_inp_path, tmp_bout_inp_dir.joinpath("BOUT.inp"))

        return tmp_bout_inp_dir

    yield _copy_inp_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


@pytest.fixture(scope="function", name="yield_bout_path_conduction")
def fixture_yield_bout_path_conduction(
    yield_conduction_path: Path,
) -> Iterator[Callable[[str], BoutPaths]]:
    """
    Make the bout_path object and clean up after use.

    Parameters
    ----------
    yield_conduction_path : Path
        The path to the conduction example

    Yields
    ------
    _make_bout_path : function
        Function which makes the BoutPaths object for the conduction example
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _make_bout_path(tmp_path_name: str) -> BoutPaths:
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
        bout_paths = BoutPaths(
            project_path=project_path, bout_inp_dst_dir=tmp_path_name
        )
        tmp_dir_list.append(bout_paths.bout_inp_dst_dir)

        return bout_paths

    yield _make_bout_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


@pytest.fixture(scope="function")
def copy_makefile(get_test_data_path: Path) -> Iterator[Path]:
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
    tmp_path = get_test_data_path.joinpath("tmp")
    tmp_path.mkdir(exist_ok=True)
    makefile_path = get_test_data_path.joinpath("Makefile")
    tmp_make = tmp_path.joinpath("my_makefile")
    shutil.copy(makefile_path, tmp_make)

    yield tmp_path

    # Teardown
    tmp_make.unlink()
    tmp_path.rmdir()


@pytest.fixture(scope="function")
def yield_number_of_rows_for_all_tables() -> Iterator[
    Callable[[DatabaseReader], Dict[str, int]]
]:
    """
    Yield the function used to count number of rows in a table.

    Yields
    ------
    _get_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    """

    def _get_number_of_rows_for_all_tables(db_reader: DatabaseReader) -> Dict[str, int]:
        """
        Return the number of rows for all tables in a schema.

        Parameters
        ----------
        db_reader : DatabaseReader
            The object used read from the database

        Returns
        -------
        number_of_rows_dict : dict
            Dict on the form

            >>> {'table_name': int}
        """
        number_of_rows_dict = dict()
        query_str = (
            "SELECT name FROM sqlite_master\n"
            "    WHERE type ='table'\n"
            "    AND name NOT LIKE 'sqlite_%'"
        )
        table_of_tables = db_reader.query(query_str)
        for _, table_name_as_series in table_of_tables.iterrows():
            table_name = table_name_as_series["name"]
            # NOTE: SQL injection possible through bad table name, however the table
            #       names are hard-coded in this example
            query_str = f"SELECT COUNT(*) AS rows FROM {table_name}"  # nosec
            table = db_reader.query(query_str)
            number_of_rows_dict[table_name] = table.loc[0, "rows"]
        return number_of_rows_dict

    yield _get_number_of_rows_for_all_tables


@pytest.fixture(scope="session")
def yield_metadata_reader(get_test_data_path: Path) -> Iterator[MetadataReader]:
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
    test_db_connection = DatabaseConnector(name="test", db_root_path=get_test_data_path)
    yield MetadataReader(test_db_connection, drop_id=None)


@pytest.fixture(scope="session")
def yield_all_metadata(get_test_data_path: Path) -> Iterator[DataFrame]:
    """
    Yield the test metadata.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    all_metadata : DataFrame
        A DataFrame containing the test metadata
    """
    dates = MetadataReader.date_columns
    all_metadata = pd.read_json(
        get_test_data_path.joinpath("all_metadata.json"),
        orient="split",
        convert_dates=dates,
    )
    yield all_metadata


@pytest.fixture(scope="session", name="yield_logs")
def fixture_yield_logs(get_test_data_path: Path) -> Iterator[Dict[str, Path]]:
    """
    Yield the different types of execution logs.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    log_paths : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = dict()
    log_paths["success_log"] = get_test_data_path.joinpath("BOUT.log.0")
    log_paths["fail_log"] = get_test_data_path.joinpath("BOUT.log.0.fail")
    log_paths["unfinished_no_pid_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_no_pid"
    )
    log_paths["unfinished_not_started_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_not_started"
    )
    log_paths["unfinished_started_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_started"
    )
    log_paths["unfinished_started_log_pid_11"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_started_pid_11"
    )

    with Path(log_paths["success_log"]).open("r") as log_file:
        # Read only the first couple of lines
        all_lines = log_file.readlines()
        unfinished_no_pid_log = "".join(all_lines[:5])
        unfinished_not_started_log = "".join(all_lines[:100])
        unfinished_started_log = "".join(all_lines[:200])
        with log_paths["unfinished_no_pid_log"].open("w") as unfinished_file:
            unfinished_file.write(unfinished_no_pid_log)
        with log_paths["unfinished_not_started_log"].open("w") as unfinished_file:
            unfinished_not_started_log = unfinished_not_started_log.replace(
                "pid: 1191", "pid: 10"
            )
            unfinished_file.write(unfinished_not_started_log)
        with log_paths["unfinished_started_log"].open("w") as unfinished_file:
            unfinished_started_log = unfinished_started_log.replace(
                "pid: 1191", "pid: 10"
            )
            unfinished_file.write(unfinished_started_log)
        with log_paths["unfinished_started_log_pid_11"].open("w") as unfinished_file:
            unfinished_started_log = unfinished_started_log.replace(
                "pid: 10", "pid: 11"
            )
            unfinished_file.write(unfinished_started_log)

    yield log_paths

    # Clean-up
    log_paths["unfinished_no_pid_log"].unlink()
    log_paths["unfinished_not_started_log"].unlink()
    log_paths["unfinished_started_log"].unlink()
    log_paths["unfinished_started_log_pid_11"].unlink()


@pytest.fixture(scope="function", name="get_test_db_copy")
def fixture_get_test_db_copy(
    get_tmp_db_dir: Path,
    get_test_data_path: Path,
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
) -> Callable[[str], DatabaseConnector]:
    """
    Return a function which returns a DatabaseConnector connected to a copy of test.db.

    Parameters
    ----------
    get_tmp_db_dir : Path
        Path to directory of temporary databases
    get_test_data_path : Path
        Path to test files
    make_test_database : DatabaseConnector
        Database connector to a database located in the temporary
        database directory

    Returns
    -------
    _get_test_db_copy : function
        Function which returns a a database connector to the copy of the test database
    """
    source = get_test_data_path.joinpath("test.db")

    def _get_test_db_copy(name: str) -> DatabaseConnector:
        """
        Return a database connector to the copy of the test database.

        Parameters
        ----------
        name : str
            Name of the temporary database

        Returns
        -------
        db_connector : DatabaseConnector
            DatabaseConnector to the copy of the test database
        """
        destination = get_tmp_db_dir.joinpath(f"{name}.db")
        shutil.copy(source, destination)
        db_connector = make_test_database(name)
        return db_connector

    return _get_test_db_copy


@pytest.fixture(scope="function")
def get_metadata_updater_and_db_reader(get_test_db_copy: Callable) -> Callable:
    """
    Return an instance of MetadataUpdater.

    The metadata_updater is connected to an isolated database

    Parameters
    ----------
    get_test_db_copy : function
        Function which returns a a database connector to the copy of the test database

    Returns
    -------
    _get_metadata_updater_and_db_reader : function
        Function which returns the MetadataUpdater object with initialized with
        connection to the database and a corresponding DatabaseReader object
    """

    def _get_metadata_updater_and_db_reader(
        name: str,
    ) -> Tuple[MetadataUpdater, DatabaseReader]:
        """
        Return a MetadataUpdater and its DatabaseConnector.

        Parameters
        ----------
        name : str
            Name of the temporary database

        Returns
        -------
        metadata_updater : MetadataUpdater
            Object to update the database with
        db_reader : DatabaseReader
            The corresponding database reader
        """
        db_connector = get_test_db_copy(name)
        db_reader = DatabaseReader(db_connector)
        metadata_updater = MetadataUpdater(db_connector, 1)
        return metadata_updater, db_reader

    return _get_metadata_updater_and_db_reader


@pytest.fixture(scope="function", name="copy_log_file")
def fixture_copy_log_file(get_test_data_path: Path) -> Iterator[Callable]:
    """
    Return a function which copy log files to a temporary directory.

    Parameters
    ----------
    get_test_data_path : Path
        Path to test files

    Yields
    ------
    _copy_log_file : function
        Function which copy log files to a temporary directory
    """
    # NOTE: This corresponds to names in test.db
    paths_to_remove = list()

    def _copy_log_file(log_file_to_copy: Path, destination_dir_name: str) -> None:
        """
        Copy log files to a temporary directory.

        Parameters
        ----------
        log_file_to_copy : Path
            Path to log file to copy
        destination_dir_name : str
            Name of directory to copy relative to the test data dir
        """
        destination_dir = get_test_data_path.joinpath(destination_dir_name)
        destination_dir.mkdir(exist_ok=True)
        destination_path = destination_dir.joinpath("BOUT.log.0")
        shutil.copy(get_test_data_path.joinpath(log_file_to_copy), destination_path)
        paths_to_remove.append(destination_dir)

    yield _copy_log_file

    for path in paths_to_remove:
        shutil.rmtree(path)


@pytest.fixture(scope="function")
def mock_pid_exists(monkeypatch: MonkeyPatch) -> Callable:
    """
    Return a function for setting up a monkeypatch of psutil.pid_exists.

    Parameters
    ----------
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest

    Returns
    -------
    mock_wrapper : function
        Function which returns a monkeypatch for psutil.pid_exists
    """

    def mock_wrapper(test_case: str):
        """
        Return monkeypatch for psutil.pid_exists.

        Note that this function wrap the mock function in order to set
        test_case

        Parameters
        ----------
        test_case : str
            Description of the test on the form

            >>> ('<log_file_present>_<pid_present_in_log>_'
            ...  '<started_time_present_in_log>_'
            ...  '<ended_time_present_in_log>_'
            ...  '<whether_pid_exists>_<new_status>')
        """

        def _pid_exists_mock(pid: Optional[int]) -> bool:
            """
            Mock psutil.pid_exists.

            Parameters
            ----------
            pid : int or None
                Processor id to check

            Returns
            -------
            bool
                Whether or not the pid exists (in a mocked form)
            """
            return (pid == 10 and "no_mock_pid" not in test_case) or pid == 11

        monkeypatch.setattr(psutil, "pid_exists", _pid_exists_mock)

    return mock_wrapper


@pytest.fixture(scope="function")
def copy_test_case_log_file(
    copy_log_file: Callable, get_test_data_path: Path, yield_logs: Dict[str, Path],
) -> Callable:
    """
    Return the function for copying the test case log files.

    Parameters
    ----------
    copy_log_file : function
        Function which copies log files
    get_test_data_path : Path
        Path to test data
    yield_logs : dict
        Dict containing paths to logs (these will be copied by copy_log_file)

    Returns
    -------
    _copy_test_case_log_file : function
        Function which copy the test case log files
    """

    def _copy_test_case_log_file(test_case: str) -> None:
        """
        Copy the test case log files.

        Parameters
        ----------
        test_case : str
            Description of the test on the form

            >>> ('<log_file_present>_<pid_present_in_log>_'
            ...  '<started_time_present_in_log>_'
            ...  '<ended_time_present_in_log>'
            ...  '_<whether_pid_exists>_<new_status>')
        """
        success_log_name = yield_logs["success_log"].name
        failed_log_name = yield_logs["fail_log"].name
        # This corresponds to the names in the `run` table in `test.db`
        name_where_status_is_running = "testdata_5"
        name_where_status_is_submitted = "testdata_6"
        copy_log_file(
            yield_logs["unfinished_started_log_pid_11"].name,
            name_where_status_is_running,
        )
        if "no_log" in test_case:
            # Copy directory and file, then deleting file in order for
            # the destructor to delete the dir
            copy_log_file(success_log_name, name_where_status_is_submitted)
            get_test_data_path.joinpath(
                name_where_status_is_submitted, success_log_name
            ).unlink()
        else:
            # A log file should be copied
            if "no_pid" in test_case:
                copy_log_file(
                    yield_logs["unfinished_no_pid_log"].name,
                    name_where_status_is_submitted,
                )
            else:
                if "not_started" in test_case:
                    copy_log_file(
                        yield_logs["unfinished_not_started_log"].name,
                        name_where_status_is_submitted,
                    )
                else:
                    if "not_ended" in test_case:
                        copy_log_file(
                            yield_logs["unfinished_started_log"].name,
                            name_where_status_is_submitted,
                        )
                    else:
                        if "error" in test_case:
                            copy_log_file(
                                failed_log_name, name_where_status_is_submitted
                            )
                        else:
                            copy_log_file(
                                success_log_name, name_where_status_is_submitted
                            )

    return _copy_test_case_log_file


# NOTE: MonkeyPatch is function scoped
@pytest.fixture(scope="function")
def get_mock_config_path(monkeypatch: MonkeyPatch) -> Iterator[Path]:
    """
    Return a mock path for the config dir and redirects get_config_path.

    Parameters
    ----------
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest

    Yields
    ------
    mock_config_path : Path
        The mocked config directory
        This will be deleted in the teardown
    """
    config_path = get_config_path()
    mock_config_path = config_path.parent.joinpath("delme_config_for_test")
    shutil.copytree(config_path, mock_config_path)

    # Redirect reading of config_files to mock_config_path for these
    # tests
    monkeypatch.setattr(
        "bout_runners.utils.paths.get_config_path", lambda: mock_config_path
    )

    yield mock_config_path

    shutil.rmtree(mock_config_path)


@pytest.fixture(scope="session")
def clean_default_db_dir() -> Iterator[Path]:
    """
    Yield the default database dir, and clean it during the teardown.

    Yields
    ------
    default_dir : Path
        Path to the default database directory
    """
    default_dir = Path().home().joinpath("BOUT_db")
    yield default_dir
    shutil.rmtree(default_dir)


@pytest.fixture(scope="function")
def make_graph() -> RunGraph:
    """
    Yield a simple graph.

    Returns
    -------
    run_graph : RunGraph
        A simple graph
    """
    run_graph = RunGraph()
    for i in range(6):
        run_graph.add_function_node(str(i))

    run_graph.add_waiting_for("4", "3")
    run_graph.add_waiting_for("5", "3")
    run_graph.add_waiting_for("3", "2")
    run_graph.add_waiting_for("2", "0")
    run_graph.add_waiting_for("1", "0")
    return run_graph


@pytest.fixture(scope="function", name="get_executor")
def fixture_get_executor(
    yield_bout_path_conduction: Callable[[str], BoutPaths]
) -> Callable[[str], Executor]:
    """
    Return a function which returns an Executor object.

    Parameters
    ----------
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction example

    Returns
    -------
    _get_executor : function
        Function which returns an Executor based on the conduction directory
    """

    def _get_executor(tmp_path_name: str) -> Executor:
        """
        Create Executor based on the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        executor : Executor
            The Executor object
        """
        bout_paths = yield_bout_path_conduction(tmp_path_name)
        executor = DefaultParameters.get_test_executor(bout_paths)

        return executor

    return _get_executor


@pytest.fixture(scope="function")
def get_bout_run_setup(
    get_executor: Callable[[str], Executor],
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
    get_default_parameters: DefaultParameters,
) -> Callable[[str], BoutRunSetup]:
    """
    Return a function which returns a BoutRunSetup object.

    Parameters
    ----------
    get_executor : function
        Function which returns an Executor based on the conduction directory
    make_test_database : function
        Function making an empty database
    get_default_parameters : DefaultParameters
        The DefaultParameters object

    Returns
    -------
    _get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """

    def _get_bout_run_setup(tmp_path_name: str) -> BoutRunSetup:
        """
        Create BoutRunSetup based on the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        bout_run_setup : BoutRunSetup
            The BoutRunSetup object
        """
        executor = get_executor(tmp_path_name)
        db_connector = make_test_database(tmp_path_name)
        final_parameters = FinalParameters(get_default_parameters)
        bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)

        return bout_run_setup

    return _get_bout_run_setup
