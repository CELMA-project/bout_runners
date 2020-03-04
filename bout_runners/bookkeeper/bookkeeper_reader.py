"""Module containing the BookkeeperReader class."""



# FIXME: You are here: Ripping apart Bookkeeper, added from
#  bookkeeper_utils and creator - remember to move the tests
# FIXME: Seems like no need to make abstract classes (except for
#  runner)

import re
import logging
import contextlib
import sqlite3
import pandas as pd
import logging
from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    cast_parameters_to_sql_type
from bout_runners.bookkeeper.bookkeeper_utils import get_db_path
from bout_runners.bookkeeper.bookkeeper_utils import tables_created
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper_connector import Bookkeeper
from bout_runners.runners.runner_utils import run_settings_run
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_file_modification
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info
from bout_runners.bookkeeper.bookkeeper_utils import \
    extract_parameters_in_use


class BookkeeperReader:

    def get_latest_row_id(self):
        """
        Return the latest row id.

        Returns
        -------
        row_id : int
            The latest row inserted row id
        """
        # https://stackoverflow.com/questions/3442033/sqlite-how-to-get-value-of-auto-increment-primary-key-after-insert-other-than
        row_id = \
            self.query('SELECT last_insert_rowid() AS id').loc[0, 'id']
        return row_id

    def check_entry_existence(self, table_name, entries_dict):
        """
        Check if the entry already exists in the table entry.

        Parameters
        ----------
        table_name : str
            Name of the table
        entries_dict : dict
            Dictionary containing the entries as key value pairs

        Parameters
        ----------
        table_name : str
            Name of the table to check
        entries_dict : dict
            The dict with entries

        Returns
        -------
        row_id : int or None
            The id of the hit. If none is found, None is returned
        """
        # NOTE: About checking for existence
        # https://stackoverflow.com/questions/9755860/valid-query-to-check-if-row-exists-in-sqlite3
        # NOTE: About SELECT 1
        # https://stackoverflow.com/questions/7039938/what-does-select-1-from-do
        where_statements = list()
        for field, val in entries_dict.items():
            where_statements.append(f'{" "*7}AND {field}="{val}"')
        where_statements[0] = where_statements[0].replace('AND',
                                                          'WHERE')
        where_statements = '\n'.join(where_statements)

        query_str = \
            (f'SELECT rowid\n'
             f'FROM {table_name}\n'
             f'WHERE\n'
             f'    EXISTS(\n'
             f'       SELECT 1\n'
             f'	      FROM {table_name}\n{where_statements})')

        table = self.query(query_str)
        row_id = None if table.empty else table.loc[0, 'rowid']

        return row_id

    def query(self, query_str):
        """
        Make a query to the database.

        Parameters
        ----------
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
        with contextlib.closing(sqlite3.connect(
                str(self.database_path))) as con:
            table = pd.read_sql_query(query_str, con)
        return table

    def tables_created(bookkeeper):
        """
        Check if the tables is created in the database.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            The Bookkeeper object

        Returns
        -------
        bool
            Whether or not the tables are created
        """
        query_str = ('SELECT name FROM sqlite_master '
                     '   WHERE type="table"')

        table = bookkeeper.query(query_str)
        return len(table.index) != 0