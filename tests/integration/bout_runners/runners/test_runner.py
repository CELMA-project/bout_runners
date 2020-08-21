"""Contains integration test for the runner."""

from pathlib import Path
from typing import Callable, Dict

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.executor import Executor
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.paths import change_directory
from tests.utils.run import assert_first_run, assert_tables_has_len_1, assert_force_run


def test_bout_runners_from_directory(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
) -> None:
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
        Function which returns the number of rows for all tables in a schema
    clean_default_db_dir : Path
        Path to the default database directory
    """
    # For automatic clean-up
    _ = clean_default_db_dir
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


def test_full_bout_runner(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
) -> None:
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
        Function which returns the number of rows for all tables in a schema
    clean_default_db_dir : Path
        Path to the default database directory
    """
    _ = clean_default_db_dir
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
