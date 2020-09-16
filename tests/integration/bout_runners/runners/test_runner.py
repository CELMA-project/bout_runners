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
        bout_run_setup = runner.run_graph["bout_run_0"]["bout_run_setup"]

    runner.run()

    bout_paths = bout_run_setup.bout_paths
    tear_down_restart_directories(bout_run_setup.bout_paths.bout_inp_dst_dir)
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
    expected_run_number = 3
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=expected_run_number,
        restarted=True,
    )
    # NOTE: The test in tests.unit.bout_runners.runners.test_bout_runner is testing
    #       restart_from_bout_inp_dst=True, whether this is testing restart_all=True
    assert_dump_files_exist(dump_dir_parent.joinpath(f"{dump_dir_name}_restart_0"))
    # ...twice
    runner.run(restart_all=True)
    expected_run_number = 4
    assert_tables_have_expected_len(
        db_reader,
        yield_number_of_rows_for_all_tables,
        expected_run_number=expected_run_number,
        restarted=True,
    )
    # NOTE: The test in tests.unit.bout_runners.runners.test_bout_runner is testing
    #       restart_from_bout_inp_dst=True, whether this is testing restart_all=True
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
        run_group.bout_paths,
        run_group.db_connector,
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
    paths = dict()
    paths["project_path"] = make_project
    paths["pre_and_post_directory"] = paths["project_path"].joinpath(
        f"pre_and_post_{name}"
    )
    paths["pre_and_post_directory"].mkdir()
    run_groups = dict()

    # RunGroup belonging to node 2
    run_groups["run_group_2"] = make_run_group(name, make_project)
    run_graph = run_groups["run_group_2"].run_graph
    paths["bout_run_directory_node_2"] = run_groups[
        "run_group_2"
    ].bout_paths.bout_inp_dst_dir

    run_groups["run_group_2"].add_pre_processor(
        {
            "function": node_zero,
            "args": (
                paths["bout_run_directory_node_2"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        }
    )
    run_groups["run_group_2"].add_pre_processor(
        {
            "function": node_one,
            "args": (
                paths["bout_run_directory_node_2"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        }
    )
    run_groups["run_group_2"].add_post_processor(
        {
            "function": node_five,
            "args": (
                paths["bout_run_directory_node_2"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        }
    )

    tear_down_restart_directories(paths["bout_run_directory_node_2"])

    # RunGroup belonging to node 3
    run_groups["run_group_3"] = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_groups["run_group_2"].bout_paths.bout_inp_dst_dir,
        waiting_for=run_groups["run_group_2"].bout_run_node_name,
    )

    # RunGroup belonging to node 4
    run_groups["run_group_4"] = make_run_group(name, make_project, run_graph)
    paths["bout_run_directory_node_4"] = run_groups[
        "run_group_4"
    ].bout_paths.bout_inp_dst_dir

    # RunGroup belonging to node 6
    run_groups["run_group_6"] = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_groups["run_group_2"].bout_paths.bout_inp_dst_dir,
        waiting_for=run_groups["run_group_2"].bout_run_node_name,
    )
    paths["bout_run_directory_node_6"] = run_groups[
        "run_group_6"
    ].bout_paths.bout_inp_dst_dir
    node_8 = run_groups["run_group_6"].add_post_processor(
        {
            "function": node_eight,
            "args": (
                paths["bout_run_directory_node_4"],
                paths["bout_run_directory_node_6"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        },
        waiting_for=run_groups["run_group_4"].bout_run_node_name,
    )

    # RunGroup belonging to node 9
    # NOTE: We need the paths['bout_run_directory_node_9'] as an input in node 7
    #       As node 9 is waiting for node 7 we hard-code the name
    #       (as we will know what it will be)
    paths["bout_run_directory_node_9"] = paths["project_path"].joinpath(
        f"{name}_restart_2"
    )
    # The function of node_seven belongs to RunGroup2, but takes
    # paths['bout_run_directory_node_9'] as an input
    node_7_name = run_groups["run_group_2"].add_post_processor(
        {
            "function": node_seven,
            "args": (
                paths["bout_run_directory_node_2"],
                paths["bout_run_directory_node_9"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        }
    )
    run_groups["run_group_9"] = make_run_group(
        name,
        make_project,
        run_graph,
        restart_from=run_groups["run_group_6"].bout_paths.bout_inp_dst_dir,
        waiting_for=(
            run_groups["run_group_4"].bout_run_node_name,
            run_groups["run_group_6"].bout_run_node_name,
            node_7_name,
        ),
    )
    run_groups["run_group_9"].add_post_processor(
        {
            "function": node_ten,
            "args": (
                paths["bout_run_directory_node_9"],
                paths["pre_and_post_directory"],
            ),
            "kwargs": None,
        },
        waiting_for=node_8,
    )

    # Run the project
    runner = BoutRunner(run_graph)
    runner.run()

    # Check that all the nodes have changed status
    with pytest.raises(RuntimeError):
        runner.run()

    # Check that all files are present
    # Check that the pre and post files are present
    for node in (0, 1, 5, 7, 8, 10):
        assert paths["pre_and_post_directory"].joinpath(f"{node}.txt").is_file()
    # Check that all the dump files are present
    for restart_str in ("", "_restart_0", "_restart_1", "_restart_2"):
        assert (
            paths["project_path"]
            .joinpath(f"{name}{restart_str}")
            .joinpath("BOUT.dmp.0.nc")
            .is_file()
            or paths["project_path"]
            .joinpath(f"{name}{restart_str}")
            .joinpath("BOUT.dmp.0.h5")
            .is_file()
        )

    # NOTE: We will only have 4 runs as node 4 is a duplicate of node 2 and will
    #       therefore be skipped
    number_of_runs = 4
    assert_tables_have_expected_len(
        DatabaseReader(run_groups["run_group_2"].db_connector),
        yield_number_of_rows_for_all_tables,
        expected_run_number=number_of_runs,
        restarted=True,
    )
