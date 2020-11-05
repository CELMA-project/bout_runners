"""Contains fixtures for file operations."""


import shutil
from pathlib import Path
from typing import Iterator, Callable, Tuple, List

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


@pytest.fixture(scope="function", name="tear_down_restart_directories")
def fixture_tear_down_restart_directories() -> Iterator[Callable[[Path], None]]:
    r"""
    Return a function for removal of restart directories.

    # FIXME: Superseed by remove directories
    # FIXME: The restart files should always be available in executor (or bout_paths)

    Yields
    ------
    _tear_down_restart_directories : function
        Function used for removal of restart directories
    """
    run_directory = list()

    def _tear_down_restart_directories(directory_group: Path) -> None:
        r"""
        Add the directory which the restart directories are based on.

        Parameters
        ----------
        directory_group : Path
            The directory which the restart directories are based on
        """
        run_directory.append(directory_group)

    yield _tear_down_restart_directories
    run_directory_parent = run_directory[0].parent
    run_directory_name = run_directory[0].name
    run_directories_list = list(
        run_directory_parent.glob(f"{run_directory_name}_restart_*")
    )
    for run_dir in run_directories_list:
        if run_dir.is_dir():
            shutil.rmtree(run_dir)


@pytest.fixture(scope="function")
def clean_up_bout_inp_src_and_dst(
    make_project: Path, tear_down_restart_directories: Callable[[Path], None]
) -> Iterator[Callable[[str, str], Tuple[Path, Path, Path]]]:
    """
    Return a function which adds temporary BOUT.inp directories to removal.

    # FIXME: Superseed by remove directories

    Warnings
    --------
    Will not clean any databases

    Parameters
    ----------
    make_project : Path
        Path to the project directory
    tear_down_restart_directories : function
        Function which add restart directories for removal

    Yields
    ------
    _clean_up_bout_inp_src_and_dst : function
        Function which adds temporary BOUT.inp directories to removal
    """
    dirs_to_delete = list()

    def _clean_up_bout_inp_src_and_dst(
        bout_inp_src_name: str, bout_inp_dst_name: str
    ) -> Tuple[Path, Path, Path]:
        """
        Add temporary BOUT.inp directories to removal.

        Parameters
        ----------
        bout_inp_src_name : str
            Name of the directory where the BOUT.inp source file resides
        bout_inp_dst_name : str
            Name of the source of the BOUT.inp destination directory

        Returns
        -------
        project_path : Path
            Path to the conduction example
        bout_inp_src_dir : Path
            Path to the BOUT.inp source dir
        bout_inp_dst_dir : Path
            Path to the BOUT.inp destination dir
        """
        project_path = make_project
        project_bout_inp = project_path.joinpath("data")
        bout_inp_src_dir = project_path.joinpath(bout_inp_src_name)
        bout_inp_dst_dir = project_path.joinpath(bout_inp_dst_name)

        tear_down_restart_directories(bout_inp_dst_dir)

        dirs_to_delete.append(bout_inp_src_dir)
        dirs_to_delete.append(bout_inp_dst_dir)

        bout_inp_src_dir.mkdir(exist_ok=True)
        shutil.copy(project_bout_inp.joinpath("BOUT.inp"), bout_inp_src_dir)

        return project_path, bout_inp_src_dir, bout_inp_dst_dir

    yield _clean_up_bout_inp_src_and_dst

    for dir_to_delete in dirs_to_delete:
        if dir_to_delete.is_dir():
            shutil.rmtree(dir_to_delete)


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
