"""Contains the SLURM integration tests for the runner."""


from pathlib import Path
from typing import Callable, Dict

import pytest

from bout_runners.database.database_reader import DatabaseReader
from bout_runners.submitter.slurm_submitter import SLURMSubmitter
from tests.utils.integration import (
    bout_runner_from_path_tester,
    full_bout_runner_tester,
    large_graph_tester,
)
from tests.utils.paths import FileStateRestorer


@pytest.mark.timeout(200)
def test_slurm_bout_runners_from_directory(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Run bout_runner_from_path_tester with SLURMSubmitter.

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
        SLURMSubmitter,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )


@pytest.mark.timeout(200)
def test_slurm_full_bout_runner(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the BoutRunner can execute a run with the SLURMSubmitter.

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
    full_bout_runner_tester(
        SLURMSubmitter,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )


@pytest.mark.timeout(600)
def test_slurm_large_graph(
    make_project: Path,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the graph with 10 nodes work as expected with the SLURMSubmitter.

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
    large_graph_tester(
        SLURMSubmitter,
        make_project,
        yield_number_of_rows_for_all_tables,
        file_state_restorer,
    )
