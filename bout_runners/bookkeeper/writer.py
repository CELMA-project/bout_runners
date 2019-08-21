import sqlite3
import contextlib
from pathlib import Path
from bout_runners.utils.file_operations import get_caller_dir


def main(database_root_path=None):
    """

    Using normalized pattern
    http://www.bkent.net/Doc/simple5.htm
    https://en.wikipedia.org/wiki/Database_normalization

    Parameters
    ----------
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path of the root caller of FIXME will
        be set to the root path

    Returns
    -------

    """

    if database_root_path is None:
        database_root_path = get_caller_dir()

    if not database_root_path.is_dir():
        database_root_path.mkdir(exist_ok=True, parents=True)

    database_path = database_root_path.join('bookkeeper.db')

    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/19524679/2786884

    with contextlib.closing(sqlite3.connect(database_path)) as con:
        with con as cur:
            cur.execute('CREATE TABLE directory ()')


if __name__ == '__main__':
    main()