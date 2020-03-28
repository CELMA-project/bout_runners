"""Module containing the DatabaseWriter class."""


import re
import logging


class DatabaseWriter:
    """
    Class for writing to the schema of the database.

    Attributes
    ----------
    database_connector : DatabaseConnector
        The database object to write to

    Methods
    -------
    create_insert_string(field_names, table_name)
        Create a question mark style string for database insertions
    insert(insert_str, values)
        Insert to the database
    create_entry(table_name, entries_dict)
        Create a database entry

    Examples
    --------
    >>> from pathlib import Path
    >>> from bout_runners.runner.runner import BoutRunner
    >>> from bout_runners.database.database_connector import \
    ...     DatabaseConnector
    >>> settings_path = Path().joinpath('path', 'to', 'BOUT.settings')
    >>> run_parameters_dict = \
    ...     BoutRunner.obtain_project_parameters(settings_path)
    >>> parameters_as_sql_types = \
    ...     BoutRunner.cast_parameters_to_sql_type(run_parameters_dict)
    >>> db_connection = DatabaseConnector('name')
    >>> db_creator = DatabaseCreator(db_connection)
    >>> db_creator.create_all_schema_tables(parameters_as_sql_types)
    >>> db_writer = DatabaseWriter(db_connection)
    >>> dummy_split_dict = {'number_of_processors': 1,
    ...                     'nodes': 2,
    ...                     'processors_per_node': 3}
    >>> db_writer.create_entry('split', dummy_split_dict)
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database object to write to
        """
        self.database_connector = database_connector

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

        self.database_connector.execute_statement(insert_str, *values)

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
        values = tuple(entries_dict.values())
        insert_str = self.create_insert_string(keys, table_name)
        self.insert(insert_str, values)

    def update_status(self):
        """Update the status."""
        # FIXME
        raise NotImplementedError('To be implemented')

