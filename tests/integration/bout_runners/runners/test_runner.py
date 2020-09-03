"""Contains integration test for the runner."""

import pytest

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
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.submitter.local_submitter import LocalSubmitter

from tests.utils.paths import change_directory
from tests.utils.run import assert_first_run, assert_tables_have_expected_len


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
        bout_run_setup = runner.run_graph.nodes["bout_run_0"]["bout_run_setup"]

    runner.run()

    bout_paths = bout_run_setup.executor.bout_paths
    db_connection = bout_run_setup.db_connector
    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connection)
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )

    # Check that all the nodes have changed status
    with pytest.raises(RuntimeError):
        runner.run()

    # Check that the run will not be executed again
    runner.reset()
    runner.run()
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )

    # Check that force overrides the behaviour
    runner.run(force=True)
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=2
    )

    # Check that the restart functionality works
    runner.run(restart=True)
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=3,
        restarted=True,
    )
    # ...twice
    runner.run(restart=True)
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=4,
        restarted=True,
    )


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
    executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )
    db_connection = DatabaseConnector(name)

    # Create the `run_group`
    run_graph = RunGraph()
    _ = RunGroup(
        run_graph, BoutRunSetup(executor, db_connection, final_parameters), name=name
    )

    # Run the project
    runner = BoutRunner(run_graph)
    runner.run()

    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connection)
    # Assert that all the values are 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )
