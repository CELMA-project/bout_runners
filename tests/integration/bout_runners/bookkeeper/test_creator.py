"""Contains the integration test for the database."""


import shutil
from pathlib import Path
import pytest
from bout_runners.database.database_creator import create_database


@pytest.fixture(scope='module', name='db_directory')
def fixture_db_directory():
    """Yield and clean the database path."""
    database_root_path = \
        Path(__file__).absolute().parent.joinpath('delme_integration')
    yield database_root_path
    shutil.rmtree(database_root_path)


def test_create_database(make_project, db_directory):
    """
    Test that the database can be made.

    Parameters
    ----------
    make_project : Path
        Path to the project
    db_directory : Path
        Path to the database
    """
    project_path = make_project
    database_root_path = db_directory
    create_database(project_path=project_path,
                    database_root_path=database_root_path)
    assert database_root_path.joinpath('conduction.db').is_file()
