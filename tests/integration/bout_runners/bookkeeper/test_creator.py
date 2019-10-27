import pytest
import shutil
from pathlib import Path
from bout_runners.bookkeeper.creator import create_database


@pytest.fixture(scope='module')
def db_directory_fixture():
    database_root_path = \
        Path(__file__).absolute().parent.joinpath('delme_integration')
    yield database_root_path
    shutil.rmtree(database_root_path)


def test_create_database(make_project, db_directory_fixture):
    project_path = make_project
    database_root_path = db_directory_fixture
    create_database(project_path, database_root_path)
    assert database_root_path.joinpath('conduction.db').is_file()
