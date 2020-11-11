"""Contains fixtures for file operations."""


import shutil
from pathlib import Path
from typing import Callable, Iterator

import pytest

from bout_runners.utils.paths import get_config_path
from tests.utils.paths import FileStateRestorer


@pytest.fixture(scope="session")
def copy_bout_inp() -> Iterator[Callable[[Path, str], Path]]:
    """
    Copy BOUT.inp to a temporary directory.

    Yields
    ------
    _copy_inp_path : function
        Function which copies BOUT.inp and returns the path to the temporary directory
    """
    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    # See:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    tmp_dir_list = []

    def _copy_inp_path(project_path: Path, tmp_path_name: str) -> Path:
        """
        Copy BOUT.inp to a temporary directory.

        Parameters
        ----------
        project_path : Path
            Root path to the project
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        tmp_bout_inp_dir : Path
            Path to the temporary directory
        """
        bout_inp_path = project_path.joinpath("data", "BOUT.inp")

        tmp_bout_inp_dir = project_path.joinpath(tmp_path_name)
        tmp_bout_inp_dir.mkdir(exist_ok=True)
        tmp_dir_list.append(tmp_bout_inp_dir)

        shutil.copy(bout_inp_path, tmp_bout_inp_dir.joinpath("BOUT.inp"))

        return tmp_bout_inp_dir

    yield _copy_inp_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


@pytest.fixture(scope="function")
def copy_makefile(get_test_data_path: Path) -> Iterator[Path]:
    """
    Set up and tear down a copy of Makefile to my_makefile.

    Creates a temporary directory, copies Makefile from DATA_PATH to
    DATA_PATH/tmp/my_makefile to search for the Makefile.
    The file and directory are teared it down after the test.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    tmp_path : Path
        The path to the temporary directory
    """
    # Setup
    tmp_path = get_test_data_path.joinpath("tmp")
    tmp_path.mkdir(exist_ok=True)
    makefile_path = get_test_data_path.joinpath("Makefile")
    tmp_make = tmp_path.joinpath("my_makefile")
    shutil.copy(makefile_path, tmp_make)

    yield tmp_path

    # Teardown
    tmp_make.unlink()
    tmp_path.rmdir()


@pytest.fixture(scope="function", name="file_state_restorer")
def fixture_file_state_restorer() -> Iterator[FileStateRestorer]:
    """
    Yield an instance of FileStateRestorer.

    Yields
    ------
    file_state_restorer : FileStateRestorer
        Object used to move files which may be in use during testing
    """
    file_state_restorer = FileStateRestorer()

    yield file_state_restorer

    file_state_restorer.restore_files()


@pytest.fixture(scope="session")
def create_mock_config_path():
    """
    Yield a mock path for the config dir and deletes it on teardown.

    Yields
    ------
    mock_config_path : Path
        The mocked config directory
        This will be deleted in the teardown
    """
    config_path = get_config_path()
    mock_config_path = config_path.parent.joinpath("delme_config_for_test")
    shutil.copytree(config_path, mock_config_path)
    yield mock_config_path
    shutil.rmtree(mock_config_path)
