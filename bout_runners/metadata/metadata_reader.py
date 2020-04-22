"""Module containing the MetadataReader class."""


import re
from datetime import datetime
from bout_runners.make.make import Make
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_utils import \
    get_file_modification
from bout_runners.database.database_utils import \
    get_system_info


class MetadataReader:
    """
    Class for reading the metadata from the database.

    FIXME
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        FIXME
        """
        self.__database_reader = DatabaseReader(database_connector)

    def get_all_metadata(self):
        """
        FIXME
        """
        pass

    def get_parameters_metadata(self, columns, table_connections):
        """
        FIXME
        """
        query = 'SELECT\n'
        for column in columns:
            query += f'{column} AS "{column}",\n'
        query += 'FROM run'
        for left_table in table_connections.keys():
            for right_table in table_connections[left_table]:
                query += (f'INNER JOIN {left_table} ON {left_table}.'
                          f'{right_table}_id = {right_table}.id\n')

        return self.__database_reader.query(query)

    @staticmethod
    def get_sorted_columns(all_column_names):
        """
        Return all columns sorted.

        The columns will be sorted alphabetically first by table
        name, then alphabetically by column name, with the exception of
        the columns from the run table, which will be presented first.

        Parameters
        ----------
        all_column_names : dict of tuple
            Dict containing the column names
            On the form
            >>> {'table_1': ('table_1_column_1', ...),
            ...  'table_2': ('table_2_column_1', ...),
            ...  'run': ('run_column_1', ...), ...}


        Returns
        -------
        sorted_columns : tuple
            Dict containing the column names
            On the form
            >>> ('run.column_name_1',
            ...  'run.column_name_2',
            ...  ...
            ...  'table_name_1.column_name_1',
            ...  'table_name_1.column_name_2', ...)
        """
        sorted_columns = list()
        table_names = sorted(all_column_names.keys())
        table_names.pop(table_names.index('run'))
        table_names.insert(0, 'run')
        for table_name in table_names:
            for column_name in sorted(all_column_names[table_name]):
                sorted_columns.append(f'{table_name}.{column_name}')
        return sorted_columns

    @staticmethod
    def get_table_connection(table_column_dict):
        """
        Return a dict containing the table connections.

        Parameters
        ----------
        table_column_dict : dict of tuple
            Dict containing the column names
            On the form
            >>> {'table_1': ('table_1_column_1', ...),
            ...  'table_2': ('table_2_column_1', ...), ...}

        Returns
        -------
        table_connection_dict : dict
            A dict telling which tables are connected to each other,
            where the key is the table under consideration and the
            value is a tuple containing the tables which have a key
            connection to the table under consideration
            On the form
            >>> {'table_1': ('table_2', 'table_3'),
            ...  'table_4': ('table_5',), ...}
        """
        table_connection_dict = dict()
        pattern = re.compile('(.*)_id')

        for table, columns in table_column_dict.items():
            ids = tuple(pattern.match(el)[1] for el in columns
                        if '_id' in el)
            if len(ids) > 0:
                table_connection_dict[table] = ids

        return table_connection_dict

    def get_all_table_names(self):
        """
        Return all the table names in the schema.

        Returns
        -------
        tuple
            A tuple containing all names of the tables
        """
        query = ("SELECT name FROM sqlite_master\n"
                 "WHERE\n"
                 "    type ='table' AND\n"
                 "    name NOT LIKE 'sqlite_%'")
        return tuple(self.__database_reader.query(query).loc[:, 'name'])

    def get_all_column_names(self, table_names):
        """
        Return all the column names of the specified tables.

        Parameters
        ----------
        table_names : tuple
            A tuple containing all names of the tables

        Returns
        -------
        table_column_dict : dict of tuple
            Dict containing the column names
            On the form
            >>> {'table_1': ('table_1_column_1', ...),
            ...  'table_2': ('table_2_column_1', ...), ...}
        """
        table_column_dict = dict()

        query = "SELECT name FROM pragma_table_info('{}')"

        for table_name in table_names:
            table_column_dict[table_name] = tuple(
                self.__database_reader.query(
                    query.format(table_name)).loc[:, 'name'])

        return table_column_dict
