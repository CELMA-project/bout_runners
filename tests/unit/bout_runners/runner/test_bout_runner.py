"""Contains unittests for the BoutRunner."""

from pathlib import Path
from typing import Callable, Dict

from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.database.database_reader import DatabaseReader
from tests.utils.paths import change_directory
from tests.utils.run import assert_first_run, assert_tables_has_len_1, assert_force_run


def test_constructor(yield_conduction_path) -> None:
    """
    Test the constructor of BoutRunner.

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example
        See the yield_conduction_path for more details
    """
    # Assert that auto setting of the setup works
    project_path = yield_conduction_path
    with change_directory(project_path):
        runner = BoutRunner()
    assert isinstance(runner.run_graph.nodes['bout_run_0']['bout_run_setup'], BoutRunSetup)

    # Assert that an empty graph can be added
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    assert len(runner.run_graph.nodes) == 0


def test_run_bout_run(make_project: Path,
                      clean_default_db_dir: Path,
                      get_bout_run_setup: Callable[[str], BoutRunSetup],
                      yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
                      ) -> None:
    """
    Test the BOUT++ run method.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    # For automatic clean-up
    _ = clean_default_db_dir
    # Make project to save time
    _ = make_project

    run_graph = RunGraph()
    runner = BoutRunner(run_graph)

    bout_run_setup = get_bout_run_setup("test_run_bout_run")
    bout_paths = bout_run_setup.executor.bout_paths
    db_connection = bout_run_setup.db_connector

    # Run once
    runner.run_bout_run(bout_run_setup)
    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connection)
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that the run will not be executed again
    runner.run_bout_run(bout_run_setup)
    # Assert that all the values are 1
    assert_tables_has_len_1(db_reader, yield_number_of_rows_for_all_tables)

    # Check that force overrides the behaviour
    runner.run_bout_run(bout_run_setup, force=True)
    assert_force_run(db_reader, yield_number_of_rows_for_all_tables)


def test_function_run() -> None:
    """Test the function run method."""
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    runner.run_function()
    runner.run_function(lambda: None)
    runner.run_function(lambda x, y: x+y, (1, 2))
    runner.run_function(lambda x, y, z=0: x+y+z, (1, 2), {"z": 3})
