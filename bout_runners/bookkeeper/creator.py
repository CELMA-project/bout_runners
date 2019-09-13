import logging
import sqlite3
import configparser
import contextlib
from pathlib import Path
from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.runners.base_runner import single_run


logger = logging.getLogger(__name__)


def obtain_project_parameters(project_path,
                              bout_inp_dir):

    single_run(execute_from_path=project_path,
               bout_inp_dir=bout_inp_dir,
               nproc=1,
               options='nout=0')

    settings_path = bout_inp_dir.joinpath('BOUT.settings')

    # The settings file lacks a header for the global parameter
    # Therefore, we add add the header [global]
    with settings_path.open('r') as f:
        settings_memory = f'[global]\n{f.read()}'

    config = configparser.ConfigParser()
    config.read_string(settings_memory)

    config.sections()

    # FIXME: YOU ARE HERE - ONE TABLE PER HEADER



def main(
         database_root_path=None):
    """
    FIXME

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
            # FIXME: Parameter attributes can be found from
            #  BOUT.settings after executing the executable once. The
            #  parameters can be read using the normal configparser
            #  library (must convert to type) https://docs.python.org/3/library/configparser.html#supported-datatypes
            # FIXME: One table per parameter section
            # FIXME: One table to reference all the parameters (join
            #  table)
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
    from dotenv import load_dotenv
    import os

    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()

    project_path = bout_path.joinpath('examples', 'conduction')
    bout_inp_dir = project_path.joinpath('data')

    obtain_project_parameters(project_path,
                              bout_inp_dir)
    # main()
