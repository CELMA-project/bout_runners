"""Global fixtures for the test routines."""

from pathlib import Path
import pytest
from bout_runners.make.make import MakeProject
from bout_runners.utils.paths import get_bout_path


@pytest.fixture(scope='session', name='get_bout_path')
def fixture_get_bout_path():
    """
    Load the dot-env file and yield the bout_path.

    Yields
    ------
    bout_path : Path
        Path to the BOUT++ repository
    """
    bout_path = get_bout_path()

    yield bout_path


@pytest.fixture(scope='session', name='make_project')
def fixture_make_project(get_bout_path):
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield
    statement

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    bout_path = get_bout_path
    project_path = bout_path.joinpath('examples', 'conduction')

    make_obj = MakeProject(makefile_root_path=project_path)
    make_obj.run_make()

    yield project_path

    # Teardown
    make_obj.run_clean()


@pytest.fixture(scope='session')
def get_test_data_path():
    """
    Return the test data path.

    Returns
    -------
    test_data_path : Path
        Path to the test data
    """
    return Path(__file__).absolute().parent.joinpath('data')
