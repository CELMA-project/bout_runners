"""Module containing the DatabaseReader class."""


import pandas as pd


class DatabaseReader:
    r"""
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
    Import dependencies
    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.parameters.default_parameters import \
    ...     DefaultParameters
    >>> from bout_runners.parameters.final_parameters import \
    ...     FinalParameters
    >>> from bout_runners.database.database_connector import \
    ...     DatabaseConnector
    >>> from bout_runners.database.database_creator import \
    ...     DatabaseCreator
    >>> from bout_runners.database.database_writer import \
    ...     DatabaseWriter

    Create the `bout_paths` object
    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source',
    ... 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination',
    ... 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Obtain the parameters
    >>> default_parameters = DefaultParameters(bout_paths)
    >>> final_parameters = FinalParameters(default_parameters)
    >>> final_parameters_dict = final_parameters.get_final_parameters()
    >>> final_parameters_as_sql_types = \
    ...     final_parameters.cast_parameters_to_sql_type(
    ...     final_parameters_dict)

    Create the database
    >>> db_connection = DatabaseConnector('name')
    >>> db_creator = DatabaseCreator(db_connection)
    >>> db_creator.create_all_schema_tables(
    ...     final_parameters_as_sql_types)

    Write to the database
    >>> db_writer = DatabaseWriter(db_connection)
    >>> dummy_split_dict = {'number_of_processors': 1,
    ...                     'number_of_nodes': 2,
    ...                     'processors_per_node': 3}
    >>> db_writer.create_entry('split', dummy_split_dict)

    Read from the database
    >>> db_reader = DatabaseReader(db_connection)
    >>> db_reader.query('SELECT * FROM split')
        id  number_of_processors  number_of_nodes  processors_per_node
     0   1                     1                1                    1

    >>> db_reader.get_latest_row_id()
    1

    >>> db_reader.get_entry_id('split', dummy_split_dict)
    1

    >>> db_reader.check_tables_created()
    True
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

    def query(self, query_str) -> pd.DataFrame:
        """
        Make a query to the database.

        Parameters
        ----------
        query_str : str
            The query to execute

        Returns
        -------
        table : pd.DataFrame
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
            val = f'"{val}"' if isinstance(val, str) else val
            where_statements.append(f'{" "*7}AND {field}={val}')
        where_statements[0] = where_statements[0].replace('AND',
                                                          'WHERE')
        where_statements = '\n'.join(where_statements)

        query_str = \
            (f'SELECT id\n'
             f'FROM {table_name}\n{where_statements}')

        table = self.query(query_str)
        # NOTE: We explicitly cast to int, as sqlite3 will cast
        #       np.int64 to bytes
        row_id = None if table.empty else int(table.loc[0, 'id'])

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
