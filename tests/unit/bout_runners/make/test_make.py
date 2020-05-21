from bout_runners.make.make import Make
from pathlib import PosixPath
from typing import Tuple

"""Contains unittests for make."""


def test_make_project(make_make_object: Tuple[Make, PosixPath]) -> None:
    """
    Test that the MakeProject class is able to make conduction.

    Parameters
    ----------
    make_make_object : tuple
        Tuple consisting of the `make_obj` (MakeProject object) and
        `exec_file` (Path). See the make_make_object fixture for details

    See Also
    --------
    make_make_object : Fixture which makes the make object
    """
    make_obj, exec_file = make_make_object

    # NOTE: The setup runs make clean, so the project directory
    #       should not contain any executable
    assert not exec_file.is_file()

    make_obj.run_make()

    assert exec_file.is_file()

    # Check that the file is not made again
    # For detail, see
    # https://stackoverflow.com/a/52858040/2786884
    first_creation_time = exec_file.stat().st_ctime

    make_obj.run_make()

    assert first_creation_time == exec_file.stat().st_ctime

    # Check that the force flag makes the project again
    make_obj.run_make(force=True)

    assert first_creation_time != exec_file.stat().st_ctime
