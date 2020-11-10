"""Module containing the DatabaseWriter class."""


import logging
import re
from typing import Any, Mapping, Sequence, Tuple, Union

from bout_runners.database.database_connector import DatabaseConnector


class DatabaseWriter:
    r"""
    Class for writing to the schema of the database.

    Attributes
    ----------
    db_connector : DatabaseConnector
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
    Import dependencies

    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.parameters.default_parameters import DefaultParameters
    >>> from bout_runners.parameters.final_parameters import FinalParameters
    >>> from bout_runners.database.database_connector import DatabaseConnector
    >>> from bout_runners.database.database_creator import DatabaseCreator

    Create the `bout_paths` object

    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Obtain the parameters

    >>> default_parameters = DefaultParameters(bout_paths)
    >>> final_parameters = FinalParameters(default_parameters)
    >>> final_parameters_dict = final_parameters.get_final_parameters()
    >>> final_parameters_as_sql_types = \
    ...     final_parameters.cast_to_sql_type(final_parameters_dict)

    Create the database

    >>> db_connector = DatabaseConnector('name', project_path)
    >>> db_creator = DatabaseCreator(db_connector)
    >>> db_creator.create_all_schema_tables(
    ...     final_parameters_as_sql_types)

    Write to the database

    >>> db_writer = DatabaseWriter(db_connector)
    >>> dummy_split_dict = {'number_of_processors': 1,
    ...                     'number_of_nodes': 2,
    ...                     'processors_per_node': 3}
    >>> db_writer.create_entry('split', dummy_split_dict)
    """

    def __init__(self, db_connector: DatabaseConnector) -> None:
        """
        Set the database to use.

        Parameters
        ----------
        db_connector : DatabaseConnector
            The database object to write to
        """
        self.db_connector = db_connector

    @staticmethod
    def create_insert_string(field_names: Sequence[str], table_name: str) -> str:
        """
        Create a question mark style string for database insertions.

        Values must be provided separately in the execution statement

        Parameters
        ----------
        field_names : array_like
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
        columns = ", ".join(field_names)
        placeholders = ", ".join("?" * len(field_names))
        insert_str = (
            f"INSERT INTO {table_name} " f"({columns}) " f"VALUES ({placeholders})"
        )
        return insert_str

    @staticmethod
    def create_update_string(
        field_names: Tuple[str, ...],
        table_name: str,
        search_condition: str,
    ) -> str:
        """
        Create a question mark style string for database update.

        Values must be provided separately in the execution statement

        Parameters
        ----------
        field_names : array_like
            Names of the fields to populate
        table_name : str
            Name of the table to use for the update
        search_condition : str
            Condition for the update
            Example

            >>> 'id = 3 AND col = 42'

        Returns
        -------
        insert_str : str
            The string to be used for update
        """
        placeholders = ""
        for col in field_names:
            placeholders += f'{" " * 4}{col} = ?,\n'
        # Remove last comma
        placeholders = f"{placeholders[:-2]}\n"

        update_str = (
            f"UPDATE {table_name}\n" f"SET\n{placeholders}" f"WHERE {search_condition}"
        )
        return update_str

    def insert(self, insert_str: str, values: Any) -> None:
        """
        Insert to the database.

        Parameters
        ----------
        insert_str : str
            The write statement to execute
        values : tuple
            Values to be inserted in the query

        Raises
        ------
        ValueError
            If the insert_str is not understood
        """
        # Obtain the table name
        pattern = r"INSERT INTO (\w*)"
        match = re.match(pattern, insert_str)
        if match is None:
            msg = f'insert_str "{insert_str}" not understood'
            logging.critical(msg)
            raise ValueError(msg)

        table_name = match.group(1)

        self.db_connector.execute_statement(insert_str, *values)

        logging.debug("Made insertion to %s", table_name)

    def update(
        self,
        update_str: str,
        values: Any,
    ) -> None:
        """
        Insert to the database.

        Parameters
        ----------
        update_str : str
            The update statement to execute
        values : tuple
            Values to be inserted in the query

        Raises
        ------
        ValueError
            If update_str is not understood
        """
        # Obtain the table name
        pattern = r"UPDATE (\w*)"
        match = re.match(pattern, update_str)

        if match is None:
            msg = f'update_str "{update_str}" not understood'
            logging.critical(msg)
            raise ValueError(msg)
        table_name = match.group(1)

        pattern = r"WHERE (.*)"
        match = re.search(pattern, update_str)

        if match is None:
            msg = f'update_str "{update_str}" not understood'
            logging.critical(msg)
            raise ValueError(msg)
        condition = match.group(1)

        self.db_connector.execute_statement(update_str, *values)

        logging.debug("Updated table %s, where %s", table_name, condition)

    def create_entry(
        self, table_name: str, entries_dict: Mapping[str, Union[int, str, float, None]]
    ) -> None:
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
        insert_str = self.create_insert_string(tuple(keys), table_name)
        self.insert(insert_str, values)
