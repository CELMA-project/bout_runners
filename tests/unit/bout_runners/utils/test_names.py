"""Contains unittests for the names module."""


import shutil
import pytest
from bout_runners.utils.names import get_exec_name
from bout_runners.utils.names import get_makefile_name
from bout_runners.utils.names import get_makefile_path


@pytest.fixture(scope='function', name='copy_makefile')
def fixture_copy_makefile(get_test_data_path):
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
    tmp_path = get_test_data_path.joinpath('tmp')
    tmp_path.mkdir(exist_ok=True)
    makefile_path = get_test_data_path.joinpath('Makefile')
    tmp_make = tmp_path.joinpath('my_makefile')
    shutil.copy(makefile_path, tmp_make)

    yield tmp_path

    # Teardown
    tmp_make.unlink()
    tmp_path.rmdir()


@pytest.mark.parametrize('filename,expected',
                         [(None, 'executable_bout_model'),
                          ('Makefile_without_target', 'bout_model')])
def test_get_exec_name(filename, expected, get_test_data_path):
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
    makefile_path = get_makefile_path(get_test_data_path,
                                      makefile_name=filename)
    exec_name = get_exec_name(makefile_path)
    assert exec_name == expected


def test_get_makefile_name(get_test_data_path):
    """
    Test that it is possible to find a makefile name.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    """
    makefile_name = get_makefile_name(get_test_data_path)
    assert makefile_name == 'Makefile'


def test_get_makefile_raises(copy_makefile):
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
