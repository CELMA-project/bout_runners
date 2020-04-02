"""Module containing the DatabaseReader class."""


import contextlib
import sqlite3
import pandas as pd


class DatabaseReader:
    """
    Class for reading the schema of the database.

    Attributes
    ----------
    database_connector : DatabaseConnector
        The database object to read from

    Methods
    -------
    query(query_str)
        Make a query to the database
    get_latest_row_id()
        Return the latest row id
    get_entry_id(table_name, entries_dict)
        Get the id of a table entry
    check_tables_created()
        Check if the tables is created in the database

    Examples
    --------
    FIXME
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database object to read from
        """
        self.database_connector = database_connector

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
        table = pd.read_sql_query(query_str,
                                  self.database_connector.connection)
        return table

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

    def get_entry_id(self, table_name, entries_dict):
        """
        Get the id of a table entry.

        Parameters
        ----------
        table_name : str
            Name of the table to check
        entries_dict : dict
            Dictionary containing the entries as key value pairs

        Returns
        -------
        row_id : int or None
            The id of the hit
            If none is found, None is returned
        """
        # NOTE: About checking for existence
        # https://stackoverflow.com/questions/9755860/valid-query-to-check-if-row-exists-in-sqlite3
        # NOTE: About SELECT 1
        # https://stackoverflow.com/questions/7039938/what-does-select-1-from-do
        where_statements = list()
        for field, val in entries_dict.items():
            val = val if isinstance(val, bool) else f'"{val}"'
            where_statements.append(f'{" "*7}AND {field}={val}')
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
        row_id = None if table.empty else table.loc[0, 'id']

        return row_id

    def check_tables_created(self):
        """
        Check if the tables is created in the database.

        Returns
        -------
        bool
            Whether or not the tables are created
        """
        query_str = ('SELECT name FROM sqlite_master '
                     '   WHERE type="table"')

        table = self.query(query_str)
        return len(table.index) != 0
