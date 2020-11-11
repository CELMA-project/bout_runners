"""Contains unittests for the BoutRunner."""


from pathlib import Path
from typing import Callable, Dict

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.runner.run_graph import RunGraph
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.utils.file_operations import copy_restart_files
from tests.utils.dummy_functions import (
    return_none,
    return_sum_of_three,
    return_sum_of_two,
)
from tests.utils.paths import FileStateRestorer, change_directory
from tests.utils.run import (
    assert_dump_files_exist,
    assert_first_run,
    assert_tables_have_expected_len,
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
    assert isinstance(runner.run_graph[node_name]["bout_run_setup"], BoutRunSetup)

    # Assert that an empty graph can be added
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    assert len(runner.run_graph.nodes) == 0


def test_run_bout_run(
    make_project: Path,
    get_bout_run_setup: Callable[[str], BoutRunSetup],
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test the BOUT++ run method.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    # Make project to save time
    _ = make_project

    run_graph = RunGraph()
    runner = BoutRunner(run_graph)

    bout_run_setup = get_bout_run_setup("test_run_bout_run")
    bout_paths = bout_run_setup.bout_paths
    db_connector = bout_run_setup.db_connector
    # NOTE: bout_run_setup.bout_paths.bout_inp_dst_dir will be removed in the
    #       yield_bout_path_conduction fixture (through the get_bout_run_setup
    #       fixture)
    #       Hence we do not need to add bout_run_setup.bout_paths.bout_inp_dst_dir
    #       to the file_state_restorer
    file_state_restorer.add(db_connector.db_path, force_mark_removal=True)

    # Run once
    submitter = bout_run_setup.submitter
    if runner.run_bout_run(bout_run_setup):
        submitter.wait_until_completed()
    # Assert that the run went well
    database_reader = assert_first_run(bout_paths, db_connector)
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        database_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )

    # Check that the run will not be executed again
    assert not runner.run_bout_run(bout_run_setup)
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        database_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )

    # Check that force overrides the behaviour
    if runner.run_bout_run(bout_run_setup, force=True):
        submitter.wait_until_completed()
    assert_tables_have_expected_len(
        database_reader, yield_number_of_rows_for_all_tables, expected_run_number=2
    )
    dump_dir_parent = bout_paths.bout_inp_dst_dir.parent
    dump_dir_name = bout_paths.bout_inp_dst_dir.name

    # Check that restart makes another entry
    bout_run_setup.executor.restart_from = bout_run_setup.bout_paths.bout_inp_dst_dir
    copy_restart_files(
        bout_run_setup.executor.restart_from, bout_run_setup.bout_paths.bout_inp_dst_dir
    )
    if runner.run_bout_run(bout_run_setup):
        submitter.wait_until_completed()
    expected_run_number = 3
    assert_tables_have_expected_len(
        database_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=expected_run_number,
        restarted=True,
    )
    # NOTE: The test in tests.unit.bout_runners.runner.test_bout_runner is testing
    #       restart_all=True, whether this is testing restart_from_bout_inp_dst=True
    assert_dump_files_exist(dump_dir_parent.joinpath(f"{dump_dir_name}_restart_0"))
    file_state_restorer.add(
        dump_dir_parent.joinpath(f"{dump_dir_name}_restart_0"), force_mark_removal=True
    )
    # ...and yet another entry
    bout_run_setup.executor.restart_from = bout_run_setup.bout_paths.bout_inp_dst_dir
    copy_restart_files(
        bout_run_setup.executor.restart_from, bout_run_setup.bout_paths.bout_inp_dst_dir
    )
    if runner.run_bout_run(bout_run_setup):
        submitter.wait_until_completed()
    assert_tables_have_expected_len(
        database_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=expected_run_number + 1,
        restarted=True,
    )
    # NOTE: The test in tests.unit.bout_runners.runner.test_bout_runner is testing
    #       restart_all=True, whether this is testing restart_from_bout_inp_dst=True
    assert_dump_files_exist(dump_dir_parent.joinpath(f"{dump_dir_name}_restart_1"))
    file_state_restorer.add(
        dump_dir_parent.joinpath(f"{dump_dir_name}_restart_1"), force_mark_removal=True
    )


def test_function_run(tmp_path: Path) -> None:
    """
    Test the function run method.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    run_graph = RunGraph()
    runner = BoutRunner(run_graph)
    path = tmp_path.joinpath("return_none.py")

    submitter = LocalSubmitter()
    runner.run_function(path, submitter, return_none)
    submitter.wait_until_completed()
    assert path.is_file()

    path = tmp_path.joinpath("return_sum_of_two.py")
    submitter = LocalSubmitter()
    runner.run_function(path, submitter, return_sum_of_two, (1, 2))
    submitter.wait_until_completed()
    assert path.is_file()

    path = tmp_path.joinpath("return_sum_of_three.py")
    submitter = LocalSubmitter()
    runner.run_function(path, submitter, return_sum_of_three, (1, 2), {"number_3": 3})
    submitter.wait_until_completed()
    assert path.is_file()
