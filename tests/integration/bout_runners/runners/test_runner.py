"""Contains integration test for the runner."""

import os
import contextlib
from pathlib import Path
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.executor import Executor
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.runner.bout_runner import BoutRunner


@contextlib.contextmanager
def change_directory(new_path):
    """
    Change working directory and return to previous directory on exit.

    Parameters
    ----------
    new_path : Path
        Path to change to

    Yields
    ------
    None

    References
    ----------
    [1] https://stackoverflow.com/a/42441759/2786884
    [2] https://stackoverflow.com/a/13197763/2786884
    """
    previous_path = Path.cwd().absolute()
    os.chdir(str(new_path))
    try:
        yield
    finally:
        os.chdir(str(previous_path))


def assert_first_run(bout_paths, db_connection):
    """
    Assert that the first run went well.

    Parameters
    ----------
    bout_paths : BoutPaths
        The object containing the paths
    db_connection : DatabaseConnector
        The database connection

    Returns
    -------
    db_reader : DatabaseReader
        The database reader object
    """
    db_reader = DatabaseReader(db_connection)
    assert bout_paths.bout_inp_dst_dir.joinpath("BOUT.dmp.0.nc").is_file()
    assert db_reader.check_tables_created()
    return db_reader


def assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables):
    """
    Assert that tables has length 1.

    Parameters
    ----------
    db_reader : DatabaseReader
        The database reader object
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    number_of_rows_dict = yield_number_of_rows_for_all_tables(db_reader)
    assert sum(number_of_rows_dict.values()) == len(number_of_rows_dict.keys())


def assert_force_run(db_reader, yield_number_of_rows_for_all_tables):
    """
    Assert that the force run is effective.

    Parameters
    ----------
    db_reader : DatabaseReader
        The database reader object
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    number_of_rows_dict = yield_number_of_rows_for_all_tables(db_reader)
    tables_with_2 = dict()
    tables_with_2["run"] = number_of_rows_dict.pop("run")
    # Assert that all the values are 1
    assert sum(number_of_rows_dict.values()) == len(number_of_rows_dict.keys())
    # Assert that all the values are 2
    assert sum(tables_with_2.values()) == 2 * len(tables_with_2.keys())


def test_bout_runners_from_directory(make_project, yield_number_of_rows_for_all_tables):
    """
    Test that the minimal BoutRunners setup works.

    This test will test that:
    1. We can execute a run from the (mocked) current work directory
    2. The metadata is properly stored
    3. We cannot execute the run again...
    4. ...unless we set force=True

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    # Make project to save time
    project_path = make_project
    with change_directory(project_path):
        runner = BoutRunner()
        runner.run()

        bout_paths = runner.executor.bout_paths
        db_connection = runner.db_connector
    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connection)
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that the run will not be executed again
    with change_directory(project_path):
        runner.run()
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that force overrides the behaviour
    with change_directory(project_path):
        runner.run(force=True)
    assert_force_run(db_reader, yield_number_of_rows_for_all_tables)


def test_full_bout_runner(make_project, yield_number_of_rows_for_all_tables):
    """
    Test that the BoutRunner can execute a run.

    This test will test that:
    1. We can execute a run
    2. The metadata is properly stored
    3. We cannot execute the run again...
    4. ...unless we set force=True

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    name = "test_bout_runner_integration"
    # Make project to save time
    project_path = make_project

    # Create the `bout_paths` object
    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=project_path.joinpath("data"),
        bout_inp_dst_dir=project_path.joinpath(name),
    )

    # Create the input objects
    run_parameters = RunParameters({"global": {"nout": 0}})
    default_parameters = DefaultParameters(bout_paths)
    final_parameters = FinalParameters(default_parameters, run_parameters)
    submitter = LocalSubmitter(bout_paths.project_path)
    executor = Executor(
        bout_paths=bout_paths, submitter=submitter, run_parameters=run_parameters,
    )
    db_connection = DatabaseConnector(name)

    # Run the project
    runner = BoutRunner(executor, db_connection, final_parameters)
    runner.run()

    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connection)
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that the run will not be executed again
    runner.run()
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that force overrides the behaviour
    runner.run(force=True)
    assert_force_run(db_reader, yield_number_of_rows_for_all_tables)
