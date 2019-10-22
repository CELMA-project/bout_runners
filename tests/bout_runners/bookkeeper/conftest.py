import shutil
from pathlib import Path

import pytest

from bout_runners.bookkeeper.bookkeeper import Bookkeeper


@pytest.fixture(scope='module')
def make_test_database():
    db_dir = Path(__file__).absolute().parents[2].joinpath('delme')
    db_dir.mkdir(exist_ok=True, parents=True)

    def _make_db(db_name):
        """
        Make a database.

        It makes sense to have one database per test as we are
        testing the content of the database

        Parameters
        ----------
        db_name : str
            Name of the database
        """
        db_path = db_dir.joinpath(db_name)
        return Bookkeeper(db_path)

    yield _make_db

    shutil.rmtree(db_dir)