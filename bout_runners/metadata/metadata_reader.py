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

    def __init__(self, database_connector, drop_id=False):
        """
        Set the database to use.

        Parameters
        ----------
        FIXME
        """
        self.__database_reader = DatabaseReader(database_connector)
        self.drop_id = drop_id

        self.__table_names = self.__get_all_table_names()
        self.__table_column_dict = self.__get_table_column_dict()
        self.__table_connections = self.__get_table_connections()
        self.__sorted_columns = self.__get_sorted_columns()

        self.__parameter_connections = \
            {'parameters': self.__table_connections['parameters']}
        self.__parameter_tables = \
            ('parameters', *self.__parameter_connections['parameters'])
        self.__parameter_columns = \
            tuple(col for col in self.__sorted_columns
                  if col.split('.')[0] in self.__parameter_tables)

    @property
    def table_names(self):
        return self.__table_names

    @property
    def table_column_dict(self):
        return self.__table_column_dict

    @property
    def table_connection(self):
        return self.__table_connections

    @property
    def sorted_columns(self):
        return self.__sorted_columns

    def drop_id(func):
        """FIXME"""
        def drop(self, *args, **kwargs):
            """FIXME"""
            dataframe = func(self, *args, **kwargs)
            if self.drop_id:
                # Remove the id's here
                pass
            return dataframe
        return drop

    @drop_id
    def get_all_metadata(self):
        """
        FIXME
        """
        parameter_query = \
            self.get_join_query('parameters',
                                self.__parameter_columns,
                                self.__parameter_columns,
                                self.__parameter_connections)

        # Adding spaces and parenthesis
        parameter_sub_query = '\n'.join([f'{" " * 6}{line}' for line in
                                        parameter_query.split('\n')])
        parameter_sub_query =\
            (f'{parameter_sub_query[:5]}({parameter_sub_query[6:-1]}) ' 
             f'AS subquery')

        # NOTE: The subquery names are the names of the columns after
        #       the query. We would like to rename them to
        #       sorted_columns. Hence the `columns` field and
        #       `alias_columns` field appears swapped
        subquery_columns = \
            [f'subquery."{col}"' if col in self.__parameter_columns
             else col
             for col in self.sorted_columns]
        # Remove the parameters from the table_connection to avoid
        # double joining
        table_connections = self.__table_connections.copy()
        table_connections.pop('parameters')
        unfinished_all_metadata_query = \
            self.get_join_query('run',
                                subquery_columns,
                                self.sorted_columns,
                                table_connections)

        # Update the parameters columns
        all_metadata_query = \
            unfinished_all_metadata_query.\
            replace(' parameters ', f'\n{parameter_sub_query}\n').\
            replace('= parameters.id', '= subquery."parameters.id"')
        # FIXME: YOU ARE HERE: Change name of decorator, let user
        #  choose, remove id AND fix dates...or...dates should always
        #  be fixed
        return self.__database_reader.query(all_metadata_query)

    @drop_id
    def get_parameters_metadata(self):
        """
        FIXME
        """
        query = self.get_join_query('parameters',
                                    self.__parameter_columns,
                                    self.__parameter_columns,
                                    self.__parameter_connections)

        return self.__database_reader.query(query)

    @staticmethod
    def get_join_query(from_statement,
                       columns,
                       alias_columns,
                       table_connections):
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

    def __get_sorted_columns(self):
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
        table_names = sorted(self.table_column_dict.keys())
        table_names.pop(table_names.index('run'))
        table_names.insert(0, 'run')
        for table_name in table_names:
            table_columns = list()
            for column_name in \
                    sorted(self.table_column_dict[table_name]):
                table_columns.append(f'{table_name}.{column_name}')
            table_columns.pop(table_columns.index(f'{table_name}.id'))
            table_columns.insert(0, f'{table_name}.id')
            sorted_columns = [*sorted_columns, *table_columns]
        return tuple(sorted_columns)

    def __get_table_connections(self):
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

        for table, columns in self.table_column_dict.items():
            ids = tuple(pattern.match(el)[1] for el in columns
                        if '_id' in el)
            if len(ids) > 0:
                table_connection_dict[table] = ids

        return table_connection_dict

    def __get_all_table_names(self):
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

    def __get_table_column_dict(self):
        """
        Return all the column names of the specified tables.

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

        for table_name in self.table_names:
            table_column_dict[table_name] = tuple(
                self.__database_reader.query(
                    query.format(table_name)).loc[:, 'name'])

        return table_column_dict
