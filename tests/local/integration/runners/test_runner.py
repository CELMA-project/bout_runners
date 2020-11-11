"""Contains local integration tests for the runner."""


from pathlib import Path
from typing import Callable, Dict

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.integration import (
    bout_runner_from_path_tester,
    full_bout_runner_tester,
    large_graph_tester,
)
from tests.utils.paths import FileStateRestorer


def test_local_bout_runners_from_directory(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Run bout_runner_from_path_tester with LocalSubmitter.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    submitter_type = LocalSubmitter

    bout_runner_from_path_tester(
        submitter_type,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )


def test_local_full_bout_runner(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the BoutRunner can execute a run with the LocalSubmitter.

    This test will test that:
    1. We can execute a run
    2. The metadata is properly stored

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    submitter_type = LocalSubmitter

    full_bout_runner_tester(
        submitter_type,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )


def test_local_large_graph(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the graph with 10 nodes work as expected with the LocalSubmitter.

    The node setup can be found in node_functions.py

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    submitter_type = LocalSubmitter

    large_graph_tester(
        submitter_type,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )
