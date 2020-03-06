"""Module containing the DatabaseReader class."""


import contextlib
import sqlite3
import pandas as pd


class DatabaseReader:
    """
    Class for reading the schema of the database.

    Attributes
    ----------
    FIXME

    Methods
    -------
    FIXME

    FIXME: Add examples
    """

    def __init__(self, database):
        """
        Set the database to use.

        Parameters
        ----------
        database : DatabaseConnector
            The database object to read from
        """
        self.database = database

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
                str(self.database.database_path))) as con:
            table = pd.read_sql_query(query_str, con)
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
            The id of the hit
            If none is found, None is returned
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

        table = self.database.query(query_str)
        return len(table.index) != 0

    def check_parameter_tables_ids(self, parameters_dict):
        """
        Insert the parameters into a the parameter tables.

        Parameters
        ----------
        parameters_dict : dict
            The dictionary on the form
            >>> {'section': {'parameter': 'value'}}

        Returns
        -------
        parameter_ids : dict
            Dict containing the ids
            The id will be None in the cases where an entry does not
            exist
            On the form
            >>> {'parameters_id': 'id_parameters', 'section': 'id', ...}

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        all_sections_got_an_id = True
        parameter_ids = dict()
        parameters_foreign_keys = dict()
        parameter_sections = list(parameters_dict.keys())

        for section in parameter_sections:
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')
            section_parameters = parameters_dict[section]
            section_id = self.get_entry_id(section_name,
                                           section_parameters)

            if section_id is None:
                all_sections_got_an_id = False

            parameter_ids[f'{section_name}_id'] = section_id

        if all_sections_got_an_id:
            parameters_id = self.get_entry_id('parameters',
                                              parameters_foreign_keys)
        else:
            parameters_id = None

        parameter_ids['parameters_id'] = parameters_id

        return parameter_ids
