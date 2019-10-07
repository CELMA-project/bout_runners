import re
import ast
import logging
import sqlite3
import configparser
import contextlib
import shutil
import pandas as pd
from pathlib import Path
from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.runners.base_runner import single_run


logger = logging.getLogger(__name__)


def obtain_project_parameters(settings_path):
    """
    FIXME: Update after FIXME below
    Returns the project parameters from the settings file

    Parameters
    ----------
    settings_path : Path
        Path to the settings file

    Returns
    -------
    parameter_dict : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value_type'}}
    """

    type_map = {'bool': 'INTEGER',  # No bool type in sqllite
                'float': 'REAL',
                'int': 'INTEGER',
                'str': 'TEXT'}

    # The settings file lacks a header for the global parameter
    # Therefore, we add add the header [global]
    with settings_path.open('r') as f:
        settings_memory = f'[global]\n{f.read()}'

    config = configparser.ConfigParser()
    config.read_string(settings_memory)

    parameter_dict = dict()

    for section in config.sections():
        parameter_dict[section] = dict()
        for key, val in config[section].items():
            # Strip comments
            capture_all_but_comment = '^([^#]*)'
            matches = re.findall(capture_all_but_comment, val, re.M)

            # Exclude comment line
            if len(matches) == 0:
                continue

            # Capitalize in case of boolean
            stripped_val = matches[0].capitalize()

            # If type is not found, type is str
            try:
                val_type = type(ast.literal_eval(stripped_val))
            except (SyntaxError, ValueError):
                val_type = str

            parameter_dict[section][key] = type_map[val_type.__name__]

    # FIXME: Bug in .settings: -d path is captured with # not in use
    parameter_dict['global'].pop('d', None)
    parameter_dict['global'].pop(str(bout_inp_dir).lower(), None)

    return parameter_dict


def run_test_run(bout_inp_dir, project_path):
    """
    Performs a test run

    Parameters
    ----------
    project_path : Path
        Path to the project
    bout_inp_dir : Path
        Path to the BOUT.inp file

    Returns
    -------
    settings_path : Path
        Path to the settings file
    """

    test_run_dir = project_path.joinpath('test_run')
    if not test_run_dir.is_dir():
        test_run_dir.mkdir(exist_ok=True, parents=True)

    settings_path = test_run_dir.joinpath('BOUT.settings')

    if not settings_path.is_file():
        test_run_inp_path = test_run_dir.joinpath('BOUT.inp')
        shutil.copy(bout_inp_dir, test_run_inp_path)

        single_run(execute_from_path=project_path,
                   bout_inp_dir=test_run_inp_path,
                   nproc=1,
                   options='nout=0')

    return settings_path


def create_table(database_path, sql_statement):
    """
    Creates a table in the database

    Parameters
    ----------
    database_path : Path or str
        Path to database
    sql_statement : str
        The query to execute
    """
    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/47501337/2786884
    # Auto-closes connection
    with contextlib.closing(sqlite3.connect(str(database_path))) as con:
        # Auto-commits
        with con as c:
            # Auto-closes cursor
            with contextlib.closing(c.cursor()) as cur:
                # Check if tables are present
                cur.execute(sql_statement)


def main(project_path=None,
         database_root_path=None):
    """
    FIXME: Global database, on schema per project - find project name

    Using normalized pattern, see [1]_ for a quick overview, and [2]_
    for a slightly deeper explanation

    Parameters
    ----------
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path will be set to $HOME/BOUT_db

    Returns
    -------

    References
    ----------
    [1] https://www.databasestar.com/database-normalization/
    [2] http://www.bkent.net/Doc/simple5.htm
    """

    if project_path is None:
        project_path = get_caller_dir()

    if database_root_path is None:
        # FIXME: Change when going to production
        database_root_path = get_caller_dir()
        # database_root_path = Path().home().joinpath('BOUT_db')

    if not database_root_path.is_dir():
        database_root_path.mkdir(exist_ok=True, parents=True)

    # NOTE: sqlite does not support schemas (except through an
    #       ephemeral ATTACH connection)
    #       https://www.sqlite.org/lang_attach.html
    #       https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
    #       Thus we will make one database per project
    schema = project_path.name
    database_path = database_root_path.joinpath(f'{schema}.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = query(database_path, query_str)

    # Check if tables are created
    if len(table.index) == 0:
        # FIXME: Create all the tables (the ones from
        #  obtain_project_parameters and the ones from below)

        # FIXME: See what files you need to version control
        file_modification_statement = \
            get_create_table_statement(name='file_modification',
                                       columns={})
        a =1

                # # FIXME: Create these
                # cur.execute('CREATE TABLE file_modification '
                #             '('
                #             '   id INTEGER PRIMARY KEY,'
                #             '   file_1_modified TIMESTAMP,'
                #             '   git_sha TEXT'
                #             ')')
                # # FIXME: Parameter attributes can be found from
                # #  BOUT.settings after executing the executable once. The
                # #  parameters can be read using the normal configparser
                # #  library (must convert to type) https://docs.python.org/3/library/configparser.html#supported-datatypes
                # # FIXME: One table per parameter section
                # # FIXME: One table to reference all the parameters (join
                # #  table)
                # cur.execute('CREATE TABLE parameters'
                #             '('
                #             '   id INTEGER PRIMARY KEY,'
                #             '   parameter_1 TEXT'
                #             ')')
                # cur.execute('CREATE TABLE split'
                #             '('
                #             '   id PRIMARY KEY,'
                #             '   nodes INTEGER,'
                #             '   processors_per_nodes INTEGER'
                #             ')')
                # cur.execute('CREATE TABLE host'
                #             '('
                #             '   id INTEGER,'
                #             '   host_name TEXT'
                #             ')')
                # cur.execute(
                #     'CREATE TABLE run '
                #     '('
                #     '   run_id INTEGER PRIMARY KEY,'
                #     '   name TEXT,'
                #     '   start TIMESTAMP,'
                #     '   stop TIMESTAMP,'
                #     '   status TEXT,'
                #     '   file_modified_id INTEGER,'
                #     '   parameters_id INTEGER,'
                #     '   split_id INTEGER,'
                #     '   host_id INTEGER,'
                #     '   FOREIGN KEY(file_modified_id) '
                #     '       REFERENCES file_modification(id),'
                #     '   FOREIGN KEY(parameters_id) '
                #     '       REFERENCES parameters(id),'
                #     '   FOREIGN KEY(split_id) REFERENCES split(id),'
                #     '   FOREIGN KEY(host_id) REFERENCES host(id))'
                # )


# FIXME: Make a sql object which contains query, insert, write etc
#  with member data database_path. Should instance hold connection
#  open? Probably not to avoid concurrency problems
def query(database_path, query_str):
    """
    Makes a query to the database specified in database_path

    Parameters
    ----------
    database_path : Path or str
        Path to database
    query_str : str
        The query to execute

    Returns
    -------
    table : DataFrame
        The result of a query as a DataFrame
    """
    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/47501337/2786884
    # Auto-closes connection
    with contextlib.closing(sqlite3.connect(str(database_path))) as con:
        df = pd.read_sql_query(query_str, con)
    return df


def get_create_table_statement(name, columns, foreign_keys=None):
    """
    Returns a SQL string which can be used to create

    Parameters
    ----------
    name : str
        Name of the table
    columns : dict
        Dictionary where the key is the column name and the value is
        the type
    foreign_keys : dict
        Dictionary where the key is the column in this table to be
        used as a foreign key and the value is the tuple
        consisting of (name_of_the_table, key_in_table) to refer to

    Returns
    -------
    create_statement : str
        The SQL statement which creates table
    """

    # FIXME: You are here
    create_statement = ''

    return create_statement


if __name__ == '__main__':
    from dotenv import load_dotenv
    import os

    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()

    project_path = bout_path.joinpath('examples', 'conduction')
    bout_inp_dir = project_path.joinpath('data')

    main(project_path)

    settings_path = run_test_run(bout_inp_dir, project_path)

    obtain_project_parameters(settings_path)
    # main()
