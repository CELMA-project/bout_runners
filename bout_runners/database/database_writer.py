"""Module containing the DatabaseWriter class."""


import re
import logging
from bout_runners.database.database_utils import \
    get_file_modification
from bout_runners.database.database_utils import \
    get_system_info
from bout_runners.database.database_utils import \
    extract_parameters_in_use


class DatabaseWriter:
    """
    Class for writing to the schema of the database.

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
            The database object to write to
        """
        self.database = database

    @staticmethod
    def create_insert_string(field_names, table_name):
        """
        Create a question mark style string for database insertions.

        Values must be provided separately in the execution statement

        Parameters
        ----------
        field_names : array-like
            Names of the fields to populate
        table_name : str
            Name of the table to use for the insertion

        Returns
        -------
        insert_str : str
            The string to be used for insertion
        """
        # From
        # https://stackoverflow.com/a/14108554/2786884
        columns = ', '.join(field_names)
        placeholders = ', '.join('?' * len(field_names))
        insert_str = f'INSERT INTO {table_name} ' \
                     f'({columns}) ' \
                     f'VALUES ({placeholders})'
        return insert_str

    def insert(self, insert_str, values):
        """
        Insert to the database.

        Parameters
        ----------
        insert_str : str
            The query to execute
        values : tuple
            Values to be inserted in the query
        """
        # Obtain the table name
        pattern = r'INSERT INTO (\w*)'
        table_name = re.match(pattern, insert_str).group(1)

        self.database.execute_statement(insert_str, values)

        logging.info('Made insertion to %s', table_name)

    def create_entry(self, table_name, entries_dict):
        """
        Create a database entry.

        Parameters
        ----------
        table_name : str
            Name of the table
        entries_dict : dict
            Dictionary containing the entries as key value pairs
        """
        keys = entries_dict.keys()
        values = entries_dict.values()
        insert_str = self.create_insert_string(keys, table_name)
        self.insert(insert_str, values)

    def update_status(self):
        """Update the status."""
        raise NotImplementedError('To be implemented')

