import pytest
from pathlib import Path
from bout_runners.utils.read_makefile import ReadBoutMakefile
from bout_runners.utils.read_makefile import BoutMakefileVariable
from bout_runners.utils.read_makefile import ReadMakefileError

DATA_PATH = Path(__file__).absolute().parents[1].joinpath('data')


def test_read_bout_makefile():
    """
    Tests that ReadBoutMakefile can read a file
    """

    reader = ReadBoutMakefile(DATA_PATH.joinpath('test_read'))
    assert reader.content == 'This is some text'


@pytest.mark.parametrize('file,expected',
                         [('Makefile_value', 'val 123 val.cxx.foo'),
                          ('Makefile_multiple_value', 'not_val')])
def test_get_variable_value(file, expected):
    """
    Tests that get_variable is reading variables properly
    """

    var = BoutMakefileVariable(DATA_PATH.joinpath(file), 'VAR')
    val = var.get_variable_value()

    assert val == expected


def test_get_variable_value_raises():
    """
    Tests that ReadMakefileError is properly raised
    """

    var = BoutMakefileVariable(DATA_PATH.joinpath(
        'Makefile_only_comment'),
        'VAR')

    with pytest.raises(ReadMakefileError):
        var.get_variable_value()
