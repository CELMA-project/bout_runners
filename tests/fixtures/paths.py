"""Contains path fixtures."""
import shutil
from pathlib import Path
from typing import Callable, Iterator

import pytest

from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.utils.paths import get_bout_directory


@pytest.fixture(scope="session")
def get_test_data_path() -> Path:
    """
    Return the test data path.

    Returns
    -------
    test_data_path : Path
        Path to the test data
    """
    return Path(__file__).absolute().parents[1].joinpath("data")


@pytest.fixture(scope="session", name="yield_bout_path")
def fixture_yield_bout_path() -> Iterator[Path]:
    """
    Yield the BOUT++ path.

    Yields
    ------
    bout_path : Path
        Path to the BOUT++ repository
    """
    bout_path = get_bout_directory()

    yield bout_path


@pytest.fixture(scope="session", name="yield_conduction_path")
def fixture_yield_conduction_path(yield_bout_path: Path) -> Iterator[Path]:
    """
    Yield the conduction path.

    Parameters
    ----------
    yield_bout_path : Path
        Path to BOUT++

    Yields
    ------
    conduction_path : Path
        Path to the BOUT++ conduction example
    """
    bout_path = yield_bout_path
    conduction_path = bout_path.joinpath("examples", "conduction")

    yield conduction_path


@pytest.fixture(scope="function")
def yield_bout_path_conduction(
    yield_conduction_path: Path,
) -> Iterator[Callable[[str], BoutPaths]]:
    """
    Make the bout_path object and clean up after use.

    Parameters
    ----------
    yield_conduction_path : Path
        The path to the conduction example

    Yields
    ------
    _make_bout_path : function
        Function which makes the BoutPaths object for the conduction example
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _make_bout_path(tmp_path_name: str) -> BoutPaths:
        """
        Create BoutPaths from the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        bout_paths : BoutPaths
            The BoutPaths object
        """
        project_path = yield_conduction_path
        bout_paths = BoutPaths(
            project_path=project_path, bout_inp_dst_dir=tmp_path_name
        )
        tmp_dir_list.append(bout_paths.bout_inp_dst_dir)

        return bout_paths

    yield _make_bout_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)
