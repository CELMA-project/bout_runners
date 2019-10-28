import pytest
import shutil
from pathlib import Path
from bout_runners.utils.exec_name import get_exec_name
from bout_runners.utils.exec_name import get_makefile_name


@pytest.mark.parametrize('file,expected',
                         [(None, 'executable_bout_model'),
                          ('Makefile_without_target', 'bout_model')])
def test_get_exec_name(file, expected, get_test_data_path):
    """
    Test that the exec name is retrievable from the makefiles
    """
    exec_name = get_exec_name(get_test_data_path, makefile_name=file)
    assert exec_name == expected


def test_get_makefile_name(get_test_data_path):
    """
    Tests that it is possible to find a makefile name
    """

    makefile_name = get_makefile_name(get_test_data_path)
    assert makefile_name == 'Makefile'


@pytest.fixture(scope='function')
def copy_makefile(get_test_data_path):
    """
    Setup and teardown sequence which copies Makefile to my_makefile

    Creates a temporary directory, copies Makefile from DATA_PATH to
    DATA_PATH/tmp/my_makefile to search for the Makefile.
    The file and directory are teared it down after the test.

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


def test_get_makefile_raises(copy_makefile):
    """
    Tests that get_makefile_name properly raises FileNotFoundError
    """

    tmp_path = copy_makefile

    with pytest.raises(FileNotFoundError):
        get_makefile_name(tmp_path)


