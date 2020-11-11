"""Contains unittests for the names module."""


from pathlib import Path
from typing import Optional

import pytest

from bout_runners.utils.names import get_exec_name, get_makefile_name, get_makefile_path


@pytest.mark.parametrize(
    "filename, expected",
    [(None, "executable_bout_model"), ("Makefile_without_target", "bout_model")],
)
def test_get_exec_name(
    filename: Optional[str], expected: str, get_test_data_path: Path
) -> None:
    """
    Test that the exec name is retrievable from the makefiles.

    Parameters
    ----------
    filename : str
        Name of the Makefile
    expected : str
        Expected name of the executable
    get_test_data_path : Path
        Path to the test data
    """
    makefile_path = get_makefile_path(get_test_data_path, makefile_name=filename)
    exec_name = get_exec_name(makefile_path)
    assert exec_name == expected


def test_get_makefile_name(get_test_data_path: Path) -> None:
    """
    Test that it is possible to find a makefile name.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    """
    makefile_name = get_makefile_name(get_test_data_path)
    assert makefile_name == "Makefile"


def test_get_makefile_raises(copy_makefile: Path) -> None:
    """
    Test that get_makefile_name properly raises FileNotFoundError.

    Parameters
    ----------
    copy_makefile : Path
        Path to the temporary Makefile
    """
    tmp_path = copy_makefile

    with pytest.raises(FileNotFoundError):
        get_makefile_name(tmp_path)
