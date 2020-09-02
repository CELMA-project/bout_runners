"""Contains unittests for BOUT paths."""


from pathlib import Path
from typing import Callable

import pytest
from bout_runners.executor.bout_paths import BoutPaths


def test_bout_path(
    yield_conduction_path: Path, copy_bout_inp: Callable[[Path, str], Path]
) -> None:
    """
    Test that BoutPath is copying files.

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example
        See the yield_conduction_path for more details
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the temporary
        directory
        See the copy_bout_inp fixture for more details
    """
    project_path = yield_conduction_path
    tmp_path_name = "tmp_BoutPath_test"

    # NOTE: We use the fixture here to automatically remove the
    # directory after the test
    tmp_path_dir = copy_bout_inp(project_path, tmp_path_name)

    # We remove the BOUT.inp to verify that BoutPaths copied the file
    tmp_path_dir.joinpath("BOUT.inp").unlink()

    bout_paths = BoutPaths(project_path=project_path, bout_inp_dst_dir=tmp_path_name)

    assert project_path.joinpath(tmp_path_name, "BOUT.inp").is_file()

    # Test the restart functionality
    with pytest.raises(FileNotFoundError):
        BoutPaths(
            project_path=project_path, bout_inp_dst_dir=tmp_path_name, restart=True
        )

    # Mock restart files
    with bout_paths.bout_inp_src_dir.joinpath("BOUT.restart.0.nc").open("w") as file:
        file.write("")

    dst_restart_file = bout_paths.bout_inp_dst_dir.joinpath("BOUT.restart.0.nc")

    bout_paths = BoutPaths(
        project_path=project_path, bout_inp_dst_dir=tmp_path_name, restart=True
    )
    assert dst_restart_file.is_file()
    bout_paths.restart = False
    assert not dst_restart_file.is_file()
    bout_paths.restart = True
    assert dst_restart_file.is_file()

    # Test that an error is raised if the source dir is pointed to a directory
    # without BOUT.inp
    with pytest.raises(FileNotFoundError):
        # NOTE: type: ignore due to https://github.com/python/mypy/issues/3004
        bout_paths.bout_inp_src_dir = "dir_without_BOUT_inp"  # type: ignore
