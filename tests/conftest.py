"""Global fixtures for the test routines."""

import os
import pytest
from pathlib import Path
from bout_runners.make.make import MakeProject
from dotenv import load_dotenv


@pytest.fixture(scope='session')
def get_bout_path():
    """
    Load the dot-env file and yield the bout_path.

    Yields
    ------
    bout_path : Path
        Path to the BOUT++ repository
    """
    # Setup
    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()

    yield bout_path


@pytest.fixture(scope='session')
def make_project(get_bout_path):
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
