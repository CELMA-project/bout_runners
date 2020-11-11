"""Contains unittests for the reading of makefiles."""


from pathlib import Path

import pytest

from bout_runners.make.read_makefile import (
    BoutMakefileReader,
    BoutMakefileVariableReader,
    MakefileReaderError,
)


def test_read_bout_makefile(get_test_data_path: Path) -> None:
    """
    Test that BoutMakefileReader can read a file.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    """
    reader = BoutMakefileReader(get_test_data_path.joinpath("test_read"))
    assert reader.content == "This is some text\n"


@pytest.mark.parametrize(
    "filename, expected",
    [("Makefile_value", "val 123 val.cxx.foo"), ("Makefile_multiple_value", "not_val")],
)
def test_get_variable_value(
    filename: str, expected: str, get_test_data_path: Path
) -> None:
    """
    Test that get_variable is reading variables properly.

    Parameters
    ----------
    filename : str
        Name of the file to run the test on
    expected : str
        Expected result
    get_test_data_path :  Path
        Path to the test data
    """
    var = BoutMakefileVariableReader(get_test_data_path.joinpath(filename), "VAR")
    val = var.value

    assert val == expected


def test_get_variable_value_raises(get_test_data_path: Path) -> None:
    """
    Test that MakefileReaderError is properly raised.

    Parameters
    ----------
    get_test_data_path :  Path
        Path to the test data
    """
    var = BoutMakefileVariableReader(
        get_test_data_path.joinpath("Makefile_only_comment"), "VAR"
    )

    with pytest.raises(MakefileReaderError):
        _ = var.value
