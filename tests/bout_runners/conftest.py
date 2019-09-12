import os
import pytest
from pathlib import Path
from bout_runners.make.make import MakeProject
from dotenv import load_dotenv


@pytest.fixture(scope='session')
def get_bout_path():
    """
    Loads the .env file and yields the bout_path

    Yields
    -------
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
    Setup and teardown sequence which makes a make object

    The method calls make_obj.run_clean() before and after the yield
    statement

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    """
    # Setup
    bout_path = get_bout_path
    project_path = bout_path.joinpath('examples', 'conduction')

    make_obj = MakeProject(makefile_root_path=project_path)
    make_obj.run_make()

    yield project_path

    # Teardown
    make_obj.run_clean()
