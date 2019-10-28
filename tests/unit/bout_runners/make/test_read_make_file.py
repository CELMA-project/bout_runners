import pytest
from pathlib import Path
from bout_runners.make.read_makefile import ReadBoutMakefile
from bout_runners.make.read_makefile import BoutMakefileVariable
from bout_runners.make.read_makefile import ReadMakefileError


def test_read_bout_makefile(get_test_data_path):
    """
    Tests that ReadBoutMakefile can read a file
    """

    reader = ReadBoutMakefile(get_test_data_path.joinpath('test_read'))
    assert reader.content == 'This is some text'


@pytest.mark.parametrize('file,expected',
                         [('Makefile_value', 'val 123 val.cxx.foo'),
                          ('Makefile_multiple_value', 'not_val')])
def test_get_variable_value(file, expected, get_test_data_path):
    """
    Tests that get_variable is reading variables properly
    """

    var = BoutMakefileVariable(get_test_data_path.joinpath(file), 'VAR')
    val = var.get_variable_value()

    assert val == expected


def test_get_variable_value_raises(get_test_data_path):
    """
    Tests that ReadMakefileError is properly raised
    """

    var = BoutMakefileVariable(get_test_data_path.joinpath(
        'Makefile_only_comment'),
        'VAR')

    with pytest.raises(ReadMakefileError):
        var.get_variable_value()
