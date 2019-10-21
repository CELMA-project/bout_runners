import pytest
import shutil
from pathlib import Path
from bout_runners.bookkeeper.bookkeeper import Bookkeeper


@pytest.fixture(scope='module')
def bookkeeper_fixture():
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


def test_create_table(bookkeeper_fixture):
    """
    Test query and create_table.

    Parameters
    ----------
    bookkeeper_fixture : Bookkeeper
        The bookkeeper from the fixture
    """
    bk = bookkeeper_fixture('create_table.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)

    # Should be empty
    assert(len(table.index) == 0)

    bk.create_table('CREATE TABLE foo (bar, TEXT)')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)

    # Should contain exactly 1
    assert(len(table.index) == 1)


def test_create_parameter_tables(bookkeeper_fixture):
    """
    Test create_parameter_tables.

    Parameters
    ----------
    bookkeeper_fixture : Bookkeeper
        The bookkeeper from the fixture
    """
    bk = bookkeeper_fixture('create_parameter_table.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)

    # Should be empty
    assert(len(table.index) == 0)

    parameter_dict = dict(foo=dict(bar='INTEGER'),
                          baz=dict(foobar='INTEGER'))

    bk.create_parameter_tables(parameter_dict)

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)

    # Should contain foo, baz and parameters
    assert(len(table.index) == 3)
