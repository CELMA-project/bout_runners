"""Contains class and methods for integration tests."""


from pathlib import Path
from typing import Callable, Dict, Type
import logging

import pytest

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from tests.utils.node_functions_complex_graph import (
    node_zero,
    node_one,
    node_five,
    node_eight,
    node_seven,
    node_ten,
)
from tests.utils.paths import change_directory
from tests.utils.run import (
    assert_first_run,
    assert_tables_have_expected_len,
    assert_dump_files_exist,
    make_run_group,
)


class LargeGraphNodeAdder:
    """
    Class used to test a complex graph.

    The node setup can be found in node_functions.py

    Attributes
    ----------
    __name : str
        Name of the first run group
    __submitter_type : type
        Used to assert that the correct submitter is used
    paths : dict
        Dictionary mapping path tags to absolute paths
    run_groups : dict
        Dictionary where names of run groups are the key and the RunGroup
        objects are values
    run_graph : RunGraph
        The RunGraph to use in BoutRunners.run()

    Methods
    -------
    add_and_assert_node_group_2()
        Add pre and post-processors to run_group_2 and assert submitter_type
    add_and_assert_node_group_3_and_4()
        Create run_group_3 and 4 and assert submitter_type
    add_and_assert_node_group_6()
        Create run_group_6, add post-processor and assert submitter_type
    add_and_assert_node_node_9(node_8)
        Create run_group_9, add post-processor and assert submitter_type
    """

    def __init__(
        self, name: str, make_project: Path, submitter_type: Type[AbstractSubmitter]
    ) -> None:
        """
        Set the member data and initialize the RunGraph.

        # FIXME: This is not automatically deleted
         (self.paths["pre_and_post_directory"]) etc.
        # FIXME: Tests are populating BOUT_db

        Parameters
        ----------
        name : str
            Name of the first run group
        make_project : Path
            The path to the conduction example
        submitter_type : type
            Used to assert that the correct submitter is used
        """
        self.__name = name
        self.__submitter_type = submitter_type
        self.paths = dict()
        self.paths["project_path"] = make_project
        self.paths["pre_and_post_directory"] = self.paths["project_path"].joinpath(
            f"pre_and_post_{name}"
        )
        self.paths["pre_and_post_directory"].mkdir()
        self.run_groups = dict()

        # Initialize the run graph
        self.run_groups["run_group_2"] = make_run_group(
            self.__name, self.paths["project_path"]
        )
        self.run_graph = self.run_groups["run_group_2"].run_graph

    def add_and_assert_node_group_2(self) -> None:
        """
        Add pre and post-processors to run_group_2 and assert submitter_type.

        Notes
        -----
        The run_group is made in the constructor in order to initialize self.run_graph

        Node 7 belongs to run_group_2, but is added in
        add_and_assert_node_node_9 as it takes
        self.paths['bout_run_directory_node_9'] as an input
        """
        self.paths["bout_run_directory_node_2"] = self.run_groups[
            "run_group_2"
        ].bout_paths.bout_inp_dst_dir
        self.run_groups["run_group_2"].add_pre_processor(
            {
                "function": node_zero,
                "args": (
                    self.paths["bout_run_directory_node_2"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            }
        )
        self.run_groups["run_group_2"].add_pre_processor(
            {
                "function": node_one,
                "args": (
                    self.paths["bout_run_directory_node_2"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            }
        )
        self.run_groups["run_group_2"].add_post_processor(
            {
                "function": node_five,
                "args": (
                    self.paths["bout_run_directory_node_2"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            }
        )
        assert isinstance(
            self.run_graph[f"bout_run_{self.__name}"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"pre_processor_{self.__name}_0"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"pre_processor_{self.__name}_1"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"post_processor_{self.__name}_0"]["submitter"],
            self.__submitter_type,
        )

    def add_and_assert_node_group_3_and_4(self) -> None:
        """Create run_group_3 and 4 and assert submitter_type."""
        # RunGroup belonging to node 3
        self.run_groups["run_group_3"] = make_run_group(
            self.__name,
            self.paths["project_path"],
            self.run_graph,
            restart_from=self.run_groups["run_group_2"].bout_paths.bout_inp_dst_dir,
            waiting_for=self.run_groups["run_group_2"].bout_run_node_name,
        )
        assert isinstance(
            self.run_graph[f"bout_run_{self.__name}_1"]["submitter"],
            self.__submitter_type,
        )
        # RunGroup belonging to node 4
        # NOTE: The name changes as there is a chance that node 4 finishes before
        #       node 0 and 1
        #       If this happens, node_zero or node_one may fail
        #       (as they are looking for .settings files)
        self.run_groups["run_group_4"] = make_run_group(
            f"a_different_{self.__name}", self.paths["project_path"], self.run_graph
        )
        self.paths["bout_run_directory_node_4"] = self.run_groups[
            "run_group_4"
        ].bout_paths.bout_inp_dst_dir
        assert isinstance(
            self.run_graph[f"bout_run_a_different_{self.__name}"]["submitter"],
            self.__submitter_type,
        )

    def add_and_assert_node_group_6(self) -> str:
        """
        Create run_group_6, add post-processor and assert submitter_type.

        Returns
        -------
        node_8 : str
            Name of node belonging to node_8
        """
        self.run_groups["run_group_6"] = make_run_group(
            self.__name,
            self.paths["project_path"],
            self.run_graph,
            restart_from=self.run_groups["run_group_2"].bout_paths.bout_inp_dst_dir,
            waiting_for=self.run_groups["run_group_2"].bout_run_node_name,
        )
        self.paths["bout_run_directory_node_6"] = self.run_groups[
            "run_group_6"
        ].bout_paths.bout_inp_dst_dir
        node_8 = self.run_groups["run_group_6"].add_post_processor(
            {
                "function": node_eight,
                "args": (
                    self.paths["bout_run_directory_node_4"],
                    self.paths["bout_run_directory_node_6"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            },
            waiting_for=self.run_groups["run_group_4"].bout_run_node_name,
        )
        assert isinstance(
            self.run_graph[f"bout_run_{self.__name}_2"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"post_processor_{self.__name}_2_0"]["submitter"],
            self.__submitter_type,
        )
        return node_8

    def add_and_assert_node_node_9(self, node_8) -> None:
        """
        Create run_group_9, add post-processor and assert submitter_type.

        Notes
        -----
        Node 7 is also added in this function as it takes
        self.paths['bout_run_directory_node_9'] as an input

        Parameters
        ----------
        node_8 : str
            Name of node belonging to node_8
        """
        # NOTE: We need the self.paths['bout_run_directory_node_9'] as an input
        #       in node 7
        #       As node 9 is waiting for node 7 we hard-code the self.__name
        #       (as we will know what it will be)
        self.paths["bout_run_directory_node_9"] = self.paths["project_path"].joinpath(
            f"{self.__name}_restart_2"
        )
        # The function of node_seven belongs to RunGroup2, but takes
        # self.paths['bout_run_directory_node_9'] as an input
        node_7_name = self.run_groups["run_group_2"].add_post_processor(
            {
                "function": node_seven,
                "args": (
                    self.paths["bout_run_directory_node_2"],
                    self.paths["bout_run_directory_node_9"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            }
        )
        self.run_groups["run_group_9"] = make_run_group(
            self.__name,
            self.paths["project_path"],
            self.run_graph,
            restart_from=self.run_groups["run_group_6"].bout_paths.bout_inp_dst_dir,
            waiting_for=(
                self.run_groups["run_group_4"].bout_run_node_name,
                self.run_groups["run_group_6"].bout_run_node_name,
                node_7_name,
            ),
        )
        self.run_groups["run_group_9"].add_post_processor(
            {
                "function": node_ten,
                "args": (
                    self.paths["bout_run_directory_node_9"],
                    self.paths["pre_and_post_directory"],
                ),
                "kwargs": None,
            },
            waiting_for=node_8,
        )
        assert isinstance(
            self.run_graph[f"post_processor_{self.__name}_1"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"bout_run_{self.__name}_3"]["submitter"],
            self.__submitter_type,
        )
        assert isinstance(
            self.run_graph[f"post_processor_{self.__name}_3_0"]["submitter"],
            self.__submitter_type,
        )


def bout_runner_from_path_tester(
    submitter_type: Type[AbstractSubmitter],
    project_path: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
    tear_down_restart_directories: Callable[[Path], None],
) -> None:
    """
    Test that the minimal BoutRunners setup works.

    This test will test that:
    1. We can execute a run from the (mocked) current work directory
    2. The correct submitter has been used
    3. The metadata is properly stored
    4. We cannot execute the run again...
    5. ...unless we set force=True
    6. Check the restart functionality twice

    Parameters
    ----------
    submitter_type : type
        Submitter type to check for
    project_path : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    clean_default_db_dir : Path
        Path to the default database directory
    tear_down_restart_directories : function
        Function used for removal of restart directories
    """
    logging.info("Start: First run")
    # For automatic clean-up
    _ = clean_default_db_dir
    # Make project to save time
    _ = project_path
    with change_directory(project_path):
        # FIXME: This will create the following files which needs to be cleaned
        # FIXME: conduction.db - may already exist - move
        # FIXME: settings_run - may already exist - move
        # FIXME: datestamped directory with run
        runner = BoutRunner()
        bout_run_setup = runner.run_graph["bout_run_0"]["bout_run_setup"]
    runner.run()
    runner.wait_until_completed()

    assert isinstance(bout_run_setup.executor.submitter, submitter_type)

    bout_paths = bout_run_setup.bout_paths
    tear_down_restart_directories(bout_run_setup.bout_paths.bout_inp_dst_dir)
    db_connector = bout_run_setup.db_connector
    # Assert that the run went well
    db_reader = assert_first_run(bout_paths, db_connector)
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )
    logging.info("Done: First run")

    logging.info("Start: Check RuntimeError")
    # Check that all the nodes have changed status
    with pytest.raises(RuntimeError):
        runner.run()
        runner.wait_until_completed()
    logging.info("Done: Check RuntimeError")
    logging.info("Start: Assert that run will not be run again")
    # Check that the run will not be executed again
    runner.reset()
    runner.run()
    runner.wait_until_completed()
    # Assert that the number of runs is 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )
    logging.info("Done: Assert that run will not be run again")
    logging.info("Start: Run with force=True")
    # Check that force overrides the behaviour
    runner.run(force=True)
    runner.wait_until_completed()
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=2
    )
    logging.info("Done: Run with force=True")

    logging.info("Start: Run with restart_all=True the first time")
    dump_dir_parent = bout_paths.bout_inp_dst_dir.parent
    dump_dir_name = bout_paths.bout_inp_dst_dir.name
    # Check that the restart functionality works
    runner.run(restart_all=True)
    runner.wait_until_completed()
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
    logging.info("Done: Run with restart_all=True the first time")
    logging.info("Start: Run with restart_all=True the second time")
    # ...twice
    runner.run(restart_all=True)
    runner.wait_until_completed()
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
    logging.info("Done: Run with restart_all=True the second time")


def full_bout_runner_tester(
    submitter_type: Type[AbstractSubmitter],
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
    submitter_type : type
        Submitter type to check for
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
    runner.wait_until_completed()

    assert isinstance(
        runner.run_graph["bout_run_test_bout_runner_integration"]["submitter"],
        submitter_type,
    )

    # Assert that the run went well
    db_reader = assert_first_run(
        run_group.bout_paths,
        run_group.db_connector,
    )
    # Assert that all the values are 1
    assert_tables_have_expected_len(
        db_reader, yield_number_of_rows_for_all_tables, expected_run_number=1
    )


def large_graph_tester(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
    tear_down_restart_directories: Callable[[Path], None],
    submitter_type: Type[AbstractSubmitter],
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
    submitter_type : type
        Used to assert that the correct submitter is used
    """
    name = f"test_large_graph_{submitter_type.__name__}"

    _ = clean_default_db_dir
    node_adder = LargeGraphNodeAdder(name, make_project, submitter_type)
    # RunGroup belonging to node 2
    node_adder.add_and_assert_node_group_2()
    tear_down_restart_directories(node_adder.paths["bout_run_directory_node_2"])
    # RunGroup belonging to node 3 and 4
    node_adder.add_and_assert_node_group_3_and_4()
    # Remember to remove run_group_4 as well since we are operating with a different
    # name
    tear_down_restart_directories(node_adder.paths["bout_run_directory_node_4"])
    # RunGroup belonging to node 6
    node_8 = node_adder.add_and_assert_node_group_6()
    # RunGroup belonging to node 9
    node_adder.add_and_assert_node_node_9(node_8)
    # Run the project
    runner = BoutRunner(node_adder.run_graph)
    runner.run()
    runner.wait_until_completed()
    # Check that all the nodes have changed status
    with pytest.raises(RuntimeError):
        runner.run()
        runner.wait_until_completed()
    # Check that all files are present
    # Check that the pre and post files are present
    for node in (0, 1, 5, 7, 8, 10):
        assert (
            node_adder.paths["pre_and_post_directory"].joinpath(f"{node}.txt").is_file()
        )
    # Check that all the dump files are present
    for restart_str in ("", "_restart_0", "_restart_1", "_restart_2"):
        assert (
            node_adder.paths["project_path"]
            .joinpath(f"{name}{restart_str}")
            .joinpath("BOUT.dmp.0.nc")
            .is_file()
            or node_adder.paths["project_path"]
            .joinpath(f"{name}{restart_str}")
            .joinpath("BOUT.dmp.0.h5")
            .is_file()
        )
    # NOTE: We will only have 4 runs as node 4 is a duplicate of node 2 and will
    #       therefore be skipped
    number_of_runs = 4
    assert_tables_have_expected_len(
        DatabaseReader(node_adder.run_groups["run_group_2"].db_connector),
        yield_number_of_rows_for_all_tables,
        expected_run_number=number_of_runs,
        restarted=True,
    )
