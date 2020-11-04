"""Contains the PBS integration tests for the runner."""


from pathlib import Path
from typing import Callable, Dict

import pytest

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.submitter.pbs_submitter import PBSSubmitter
from tests.utils.integration import (
    bout_runner_from_path_tester,
    full_bout_runner_tester,
    large_graph_tester,
)


# @pytest.mark.timeout(200)
# def test_bout_runners_from_directory(
#     make_project: Path,
#     yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
#     clean_default_db_dir: Path,
#     tear_down_restart_directories: Callable[[Path], None],
# ) -> None:
#     """
#     Test that the minimal BoutRunners setup works with the PBSSubmitter.
#
#     This test will test that:
#     1. We can execute a run from the (mocked) current work directory
#     2. The correct submitter has been used
#     3. The metadata is properly stored
#     4. We cannot execute the run again...
#     5. ...unless we set force=True
#     6. Check the restart functionality twice
#
#     Parameters
#     ----------
#     make_project : Path
#         The path to the conduction example
#     yield_number_of_rows_for_all_tables : function
#         Function which returns the number of rows for all tables in a schema
#     clean_default_db_dir : Path
#         Path to the default database directory
#     tear_down_restart_directories : function
#         Function used for removal of restart directories
#     """
#     bout_runner_from_path_tester(
#         PBSSubmitter,
#         make_project,
#         yield_number_of_rows_for_all_tables,
#         clean_default_db_dir,
#         tear_down_restart_directories,
#     )


# @pytest.mark.timeout(200)
# def test_full_bout_runner(
#     make_project: Path,
#     yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
#     clean_default_db_dir: Path,
# ) -> None:
#     """
#     Test that the BoutRunner can execute a run with the PBSSubmitter.
#
#     This test will test that:
#     1. We can execute a run
#     2. The metadata is properly stored
#
#     Parameters
#     ----------
#     make_project : Path
#         The path to the conduction example
#     yield_number_of_rows_for_all_tables : function
#         Function which returns the number of rows for all tables in a schema
#     clean_default_db_dir : Path
#         Path to the default database directory
#     """
#     full_bout_runner_tester(
#         PBSSubmitter,
#         make_project,
#         yield_number_of_rows_for_all_tables,
#         clean_default_db_dir,
#     )
#
#
def test_large_graph(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    clean_default_db_dir: Path,
    tear_down_restart_directories: Callable[[Path], None],
) -> None:
    """
    Test that the graph with 10 nodes work as expected with the PBSSubmitter.

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
    large_graph_tester(
        make_project,
        yield_number_of_rows_for_all_tables,
        clean_default_db_dir,
        tear_down_restart_directories,
        PBSSubmitter,
    )
