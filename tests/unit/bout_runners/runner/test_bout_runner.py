"""Contains unittests for the BoutRunner."""

from pathlib import Path
from typing import Callable, Dict

from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.database.database_reader import DatabaseReader
from tests.utils.paths import change_directory
from tests.utils.run import assert_first_run, assert_tables_has_len_1, assert_force_run
from tests.utils.dummy_functions import (
    return_none,
    return_sum_of_two,
    return_sum_of_three,
)


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

    node_name = list(runner.run_graph.nodes.keys())[0]
    assert isinstance(runner.run_graph.nodes[node_name]["bout_run_setup"], BoutRunSetup)

    # Assert that an empty graph can be added
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    assert len(runner.run_graph.nodes) == 0


def test_run_bout_run(
    make_project: Path,
    clean_default_db_dir: Path,
    get_bout_run_setup: Callable[[str], BoutRunSetup],
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
) -> None:
    """
    Test the BOUT++ run method.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    clean_default_db_dir : Path
        Path to the default database dir
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
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


def test_function_run(tmp_path: Path) -> None:
    """
    Test the function run method.

    Parameters
    ----------
    tmp_path : Path
        Temporary path
    """
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    path = tmp_path.joinpath("return_none.py")

    runner.run_function(path, return_none)
    assert path.is_file()

    path = tmp_path.joinpath("return_sum_of_two.py")
    runner.run_function(path, return_sum_of_two, (1, 2))
    assert path.is_file()

    path = tmp_path.joinpath("return_sum_of_three.py")
    runner.run_function(path, return_sum_of_three, (1, 2), {"number_3": 3})
    assert path.is_file()
