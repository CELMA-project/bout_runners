import sqlite3
import contextlib
from pathlib import Path
from bout_runners.utils.file_operations import get_caller_dir


def main(database_root_path=None):
    """

    Using normalized pattern, see [1]_ for a quick overview, and [2]_
    for a slightly deeper explanation

    Parameters
    ----------
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path of the root caller of FIXME will
        be set to the root path

    Returns
    -------

    References
    ----------
    [1] https://www.databasestar.com/database-normalization/
    [2] http://www.bkent.net/Doc/simple5.htm
    """

    if database_root_path is None:
        database_root_path = get_caller_dir()

    if not database_root_path.is_dir():
        database_root_path.mkdir(exist_ok=True, parents=True)

    database_path = database_root_path.joinpath('bookkeeper.db')

    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/19524679/2786884

    with contextlib.closing(sqlite3.connect(str(database_path))) as con:
        with con as cur:
            # FIXME: Check if table already exist
            # FIXME: Files depend on project
            cur.execute('CREATE TABLE file_modification '
                        '('
                        '   id INTEGER PRIMARY KEY,'
                        '   file_1_modified TIMESTAMP,'
                        '   git_sha TEXT'
                        ')')
            # FIXME: Parameters depend on project
            cur.execute('CREATE TABLE parameters'
                        '('
                        '   id INTEGER PRIMARY KEY,'
                        '   parameter_1 TEXT'
                        ')')
            cur.execute('CREATE TABLE split'
                        '('
                        '   id PRIMARY KEY,'
                        '   nodes INTEGER,'
                        '   processors_per_nodes INTEGER'
                        ')')
            cur.execute('CREATE TABLE host'
                        '('
                        '   id INTEGER,'
                        '   host_name TEXT'
                        ')')
            cur.execute(
                'CREATE TABLE run '
                '('
                '   run_id INTEGER PRIMARY KEY,'
                '   name TEXT,'
                '   start TIMESTAMP,'
                '   stop TIMESTAMP,'
                '   status TEXT,'
                '   file_modified_id INTEGER,'
                '   parameters_id INTEGER,'
                '   split_id INTEGER,'
                '   host_id INTEGER,'
                '   FOREIGN KEY(file_modified_id) '
                '       REFERENCES file_modification(id),'
                '   FOREIGN KEY(parameters_id) '
                '       REFERENCES parameters(id),'
                '   FOREIGN KEY(split_id) REFERENCES split(id),'
                '   FOREIGN KEY(host_id) REFERENCES host(id))'
            )


if __name__ == '__main__':
    main()