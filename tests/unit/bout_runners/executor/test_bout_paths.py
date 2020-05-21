"""Contains unittests for BOUT paths."""


import pytest
from bout_runners.executor.bout_paths import BoutPaths
from pathlib import Path
from typing import Callable


def test_bout_path(yield_conduction_path: Path, copy_bout_inp: Callable) -> None:
    """
    Test that BoutPath is copying BOUT.inp.

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the yield_conduction_path for more details
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory. See the copy_bout_inp fixture for
        more details.
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

    with pytest.raises(FileNotFoundError):
        bout_paths.bout_inp_src_dir = "dir_without_BOUT_inp"
