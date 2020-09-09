"""Contains integration test for the runner."""

import pytest

from pathlib import Path
from typing import Callable, Dict

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.runner.bout_runner import BoutRunner

from tests.utils.node_functions import (
    node_zero,
    node_one,
    node_five,
    node_seven,
    node_eight,
    node_ten,
)
from tests.utils.paths import change_directory
from tests.utils.run import (
    assert_first_run,
    assert_dump_files_exist,
    assert_tables_have_expected_len,
    make_run_group,
)


def test_bout_runners_from_directory(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
    tear_down_restart_directories: Callable[[Path], None],
) -> None:
    """
    Test that the minimal BoutRunners setup works.

    This test will test that:
    1. We can execute a run from the (mocked) current work directory
    2. The metadata is properly stored
    3. We cannot execute the run again...
    4. ...unless we set force=True
    5. Check the restart functionality twice

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    clean_default_db_dir : Path
        Path to the default database directory
    tear_down_restart_directories : function
        Function used for removal of restart directories
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
    tear_down_restart_directories(bout_run_setup.executor.bout_paths.bout_inp_dst_dir)
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

    dump_dir_parent = bout_paths.bout_inp_dst_dir.parent
    dump_dir_name = bout_paths.bout_inp_dst_dir.name

    # Check that the restart functionality works
    runner.run(restart_all=True)
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=3,
        restarted=True,
    )
    assert_dump_files_exist(dump_dir_parent.joinpath(f"{dump_dir_name}_restart_0"))
    # ...twice
    runner.run(restart_all=True)
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=4,
        restarted=True,
    )
    assert_dump_files_exist(dump_dir_parent.joinpath(f"{dump_dir_name}_restart_1"))


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
    run_group = make_run_group(name, make_project)

    # Run the project
    runner = BoutRunner(run_group.run_graph)
    runner.run()

    # Assert that the run went well
    db_reader = assert_first_run(
        run_group.bout_run_setup.executor.bout_paths,
        run_group.bout_run_setup.db_connector,
    )
    # Assert that all the values are 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )


def test_large_graph(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
    tear_down_restart_directories: Callable[[Path], None],
) -> None:
    """
    Test that the graph with 10 nodes work as expected.

    The node setup can be found in node_functions.py

    # FIXME: You are here. 1. Run the graph, 2. check files, 3. check db, 4. check graph

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    clean_default_db_dir : Path
        Path to the default database directory
    tear_down_restart_directories : function
        Function used for removal of restart directories
    """
    _ = clean_default_db_dir
    name = "test_large_graph"

    # RunGroup belonging to node 2
    run_group_2 = make_run_group(name, make_project)
    run_graph = run_group_2.run_graph
    project_path = make_project
    pre_and_post_directory = project_path.joinpath(f"pre_and_post_{name}")
    pre_and_post_directory.mkdir()
    bout_run_directory_node_2 = (
        run_group_2.bout_run_setup.executor.bout_paths.bout_inp_dst_dir
    )
    tear_down_restart_directories(bout_run_directory_node_2)
    run_group_2.add_pre_processor(
        {
            "function": node_zero,
            "args": (bout_run_directory_node_2, pre_and_post_directory),
            "kwargs": None,
        }
    )
    run_group_2.add_pre_processor(
        {
            "function": node_one,
            "args": (bout_run_directory_node_2, pre_and_post_directory),
            "kwargs": None,
        }
    )
    run_group_2.add_post_processor(
        {
            "function": node_five,
            "args": (bout_run_directory_node_2, pre_and_post_directory),
            "kwargs": None,
        }
    )

    # RunGroup belonging to node 3
    _ = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_group_2.bout_run_setup.executor.bout_paths.bout_inp_dst_dir,
        waiting_for=run_group_2.bout_run_node_name,
    )

    # RunGroup belonging to node 4
    run_group_4 = make_run_group(name, make_project)
    bout_run_directory_node_4 = (
        run_group_4.bout_run_setup.executor.bout_paths.bout_inp_dst_dir
    )

    # RunGroup belonging to node 6
    run_group_6 = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_group_2.bout_run_setup.executor.bout_paths.bout_inp_dst_dir,
        waiting_for=run_group_2.bout_run_node_name,
    )
    bout_run_directory_node_6 = (
        run_group_6.bout_run_setup.executor.bout_paths.bout_inp_dst_dir
    )
    node_8 = run_group_6.add_post_processor(
        {
            "function": node_eight,
            "args": (
                bout_run_directory_node_4,
                bout_run_directory_node_6,
                pre_and_post_directory,
            ),
            "kwargs": None,
        },
        waiting_for=run_group_4.bout_run_node_name,
    )

    # RunGroup belonging to node 9
    # NOTE: We need the bout_run_directory_node_9 as an input in node 7
    #       As node 9 is waiting for node 7 we hard-code the name (as we will know what it will be)
    bout_run_directory_node_9 = project_path.joinpath(f"{name}_4")
    # The function of node_seven belongs to RunGroup2, but takes bout_run_directory_node_9 as an input
    node_7_name = run_group_2.add_post_processor(
        {
            "function": node_seven,
            "args": (
                bout_run_directory_node_2,
                bout_run_directory_node_9,
                pre_and_post_directory,
            ),
            "kwargs": None,
        }
    )
    run_group_9 = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_group_6.bout_run_setup.executor.bout_paths.bout_inp_dst_dir,
        waiting_for=(
            run_group_4.bout_run_node_name,
            run_group_6.bout_run_node_name,
            node_7_name,
        ),
    )
    run_group_9.add_post_processor(
        {
            "function": node_ten,
            "args": (bout_run_directory_node_9, pre_and_post_directory),
            "kwargs": None,
        },
        waiting_for=node_8,
    )

    # Run the project
    runner = BoutRunner(run_graph)
    runner.run()

    # Assert that the run went well
    db_reader = assert_first_run(
        run_group_2.bout_run_setup.executor.bout_paths,
        run_group_2.bout_run_setup.db_connector,
    )
    # Assert that all the values are 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )
