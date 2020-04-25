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

    def get_all_metadata(self, columns, table_connections,
                         sorted_columns):
        """
        FIXME
        """
        # FIXME: This first part needs heavy refactoring
        parameter_connections = {'parameters':
                                 table_connections.pop('parameters')}
        parameter_tables = \
            ('parameters', *parameter_connections['parameters'])
        parameter_columns = \
            [col for col in sorted_columns
             if col.split('.')[0] in parameter_tables]

        parameter_query = \
            self.get_join_query('parameters',
                                parameter_columns,
                                parameter_columns,
                                parameter_connections)




        # Adding spaces
        parameter_query = '\n'.join([f'{" " * 6}{line}' for line in
                                     parameter_query.split('\n')])

        # Adding parenthesis
        parameter_query =\
            f'{parameter_query[:5]}({parameter_query[6:-1]}) AS ' \
            f'subquery'
        from_statement = 'run'
        alias_columns = \
            [f'subquery."{col}"' if col in parameter_columns else col
             for col in columns]
        # FIXME: Swapped alias and columns here
        unfinished_all_metadata_query = \
            self.get_join_query(from_statement,
                                alias_columns,
                                columns,
                                table_connections)

        all_metadata_query = \
            unfinished_all_metadata_query.\
            replace(' parameters ', f'\n{parameter_query}\n').\
            replace('= parameters.id', '= subquery."parameters.id"')
        return self.__database_reader.query(all_metadata_query)

    def get_parameters_metadata(self, columns, table_connections):
        """
        FIXME
        """
        query = self.get_join_query('parameters',
                                    columns,
                                    columns,
                                    table_connections)

        return self.__database_reader.query(query)

    @staticmethod
    def get_join_query(from_statement, columns,
                       alias_columns, table_connections):
        query = 'SELECT\n'
        for column, alias in zip(columns, alias_columns):
            query += f'{" " * 7}{column} AS "{alias}",\n'
        # Remove last comma
        query = f'{query[:-2]}\n'
        query += f'FROM {from_statement}\n'
        for left_table in table_connections.keys():
            for right_table in table_connections[left_table]:
                query += (f'{" " * 4}INNER JOIN {right_table} ON '
                          f'{left_table}.'
                          f'{right_table}_id = {right_table}.id\n')
        return query

    @staticmethod
    def get_sorted_columns(table_column_dict):
        """
        Return all columns sorted.

        The columns will be sorted alphabetically first by table
        name, then alphabetically by column name, with the
        following exceptions:

        1. The columns from the run table is presented first
        2. The id column is the first column in the table

        Parameters
        ----------
        table_column_dict : dict of tuple
            Dict containing the column names
            On the form
            >>> {'table_1': ('table_1_column_1', ...),
            ...  'table_2': ('table_2_column_1', ...),
            ...  'run': ('run_column_1', 'id', ...), ...}

        Returns
        -------
        sorted_columns : tuple
            Dict containing the column names
            On the form
            >>> ('run.id',
            ...  'run.column_name_1',
            ...  'run.column_name_2',
            ...  ...
            ...  'table_name_1.column_name_1',
            ...  'table_name_1.column_name_2', ...)
        """
        sorted_columns = list()
        table_names = sorted(table_column_dict.keys())
        table_names.pop(table_names.index('run'))
        table_names.insert(0, 'run')
        for table_name in table_names:
            table_columns = list()
            for column_name in sorted(table_column_dict[table_name]):
                table_columns.append(f'{table_name}.{column_name}')
            table_columns.pop(table_columns.index(f'{table_name}.id'))
            table_columns.insert(0, f'{table_name}.id')
            sorted_columns = [*sorted_columns, *table_columns]
        return tuple(sorted_columns)

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

    def get_table_column_dict(self, table_names):
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
