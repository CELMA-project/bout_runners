import pytest
import shutil
from pathlib import Path
from bout_runners.utils.exec_name import get_exec_name
from bout_runners.utils.exec_name import get_makefile_name


DATA_PATH = Path(__file__).absolute().parents[1].joinpath('data')


@pytest.mark.parametrize('file,expected',
                         [(None, 'executable_bout_model'),
                          ('Makefile_without_target', 'bout_model')])
def test_get_exec_name(file, expected):
    """
    Test that the exec name is retrievable from the makefiles
    """
    exec_name = get_exec_name(DATA_PATH, makefile_name=file)
    assert exec_name == expected


def test_get_makefile_name():
    """
    Tests that it is possible to find a makefile name
    """

    makefile_name = get_makefile_name(DATA_PATH)
    assert makefile_name == 'Makefile'


@pytest.fixture(scope='function')
def copy_makefile():
    """
    Setup and teardown sequence which copies Makefile to my_makefile

    Creates a temporary directory, copies Makefile from DATA_PATH to
    DATA_PATH/tmp/my_makefile to search for the Makefile.
    The file and directory are teared it down after the test.
    """
    # Setup
    tmp_path = DATA_PATH.joinpath('tmp')
    tmp_path.mkdir(exist_ok=True)
    makefile_path = DATA_PATH.joinpath('Makefile')
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


