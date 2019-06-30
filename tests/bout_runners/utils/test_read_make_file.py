import pytest
from pathlib import Path
from bout_runners.utils.read_make_file import ReadBoutMakeFile
from bout_runners.utils.read_make_file import BoutMakeFileVariable
from bout_runners.utils.read_make_file import ReadMakeFileError

DATA_PATH = Path(__file__).absolute().parents[1].joinpath('data')


def test_read_bout_make_file():
    """
    Tests that ReadBoutMakeFile can read a file
    """

    reader = ReadBoutMakeFile(DATA_PATH.joinpath('test_read'))
    assert reader.content == 'This is some text'


@pytest.mark.parametrize("file,expected",
                         [('Makefile_value', 'val 123 val.cxx.foo'),
                          ('Makefile_multiple_value', 'not_val')])
def test_get_variable_value(file, expected):
    """
    Tests that get_variable is reading variables properly
    """

    var = BoutMakeFileVariable(DATA_PATH.joinpath(file), 'VAR')
    val = var.get_variable_value()

    assert val == expected


def test_get_variable_value_raises():
    """
    Tests that ReadMakeFileError is properly raised
    """

    var = BoutMakeFileVariable(DATA_PATH.joinpath(
        'Makefile_only_comment'),
        'VAR')

    with pytest.raises(ReadMakeFileError):
        var.get_variable_value()
