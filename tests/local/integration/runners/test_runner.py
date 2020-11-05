"""Contains local integration tests for the runner."""


from pathlib import Path
from typing import Callable, Dict

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.paths import FileStateRestorer
from tests.utils.integration import (
    bout_runner_from_path_tester,
    full_bout_runner_tester,
    large_graph_tester,
)


def test_bout_runners_from_directory(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Run bout_runner_from_path_tester.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    bout_runner_from_path_tester(
        LocalSubmitter,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )


def test_full_bout_runner(
    tmp_path: Path,
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
) -> None:
    """
    Test that the BoutRunner can execute a run with the LocalSubmitter.

    This test will test that:
    1. We can execute a run
    2. The metadata is properly stored

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    """
    full_bout_runner_tester(
        tmp_path, LocalSubmitter, make_project, yield_number_of_rows_for_all_tables
    )


def test_large_graph(
    tmp_path: Path,
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    tear_down_restart_directories: Callable[[Path], None],
) -> None:
    """
    Test that the graph with 10 nodes work as expected with the LocalSubmitter.

    The node setup can be found in node_functions.py

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    tear_down_restart_directories : function
        Function used for removal of restart directories
    """
    submitter_type = LocalSubmitter

    large_graph_tester(
        tmp_path,
        make_project,
        yield_number_of_rows_for_all_tables,
        tear_down_restart_directories,
        submitter_type,
    )
