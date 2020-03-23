"""Global fixtures for the test routines."""

import shutil
from pathlib import Path
import pytest
from bout_runners.make.make import MakeProject
from bout_runners.utils.paths import get_bout_path
from bout_runners.database.database_connector import DatabaseConnector


@pytest.fixture(scope='session', name='yield_bout_path')
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


@pytest.fixture(scope='session', name='yield_conduction_path')
def fixture_get_conduction_path(yield_bout_path):
    """
    Yield the conduction path.

    Yields
    ------
    conduction_path : Path
        Path to the BOUT++ conduction example
    """
    bout_path = yield_bout_path
    conduction_path = bout_path.joinpath('examples', 'conduction')

    yield conduction_path


@pytest.fixture(scope='session', name='make_project')
def fixture_make_project(yield_conduction_path):
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield
    statement

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the fixture_get_conduction_path for more details

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    project_path = yield_conduction_path

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


@pytest.fixture(scope='session')
def make_test_database():
    """
    Return the wrapped function.

    Yields
    ------
    _make_db : function
        The function making the database
    """
    db_dir = Path(__file__).absolute().parents[2].joinpath('delme')
    db_dir.mkdir(exist_ok=True, parents=True)

    def _make_db(db_name=None):
        """
        Make a database.

        It makes sense to have one database per test as we are
        testing the content of the database

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        Database
            The database object
        """
        return DatabaseConnector(name=db_name,
                                 database_root_path=db_dir)

    yield _make_db

    shutil.rmtree(db_dir)
