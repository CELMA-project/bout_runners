"""Module containing the Bookkeeper class."""


import re
import logging
import contextlib
import sqlite3
import pandas as pd
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    extract_parameters_in_use


class Bookkeeper:
    """
    Class interacting with the database.

    Attributes
    ----------
    database_path : Path or str
        Path to database

    Methods
    -------
    create_table(sql_statement)
        Create a table for each BOUT.settings section and a join table
    query(query_str)
        Make a query to the database
    """

    def __init__(self, database_path):
        """
        Set the path to the data base.

        Parameters
        ----------
        database_path : Path or str
            Path to database
        """
        self.database_path = database_path
        logging.info('Database path set to %s', self.database_path)

    def create_table(self, table_str):
        """
        Create a table in the database.

        Parameters
        ----------
        table_str : str
            The query to execute
        """
        # Obtain the table name
        pattern = r'CREATE TABLE (\w*)'
        table_name = re.match(pattern, table_str).group(1)

        self.execute_statement(table_str)

        logging.info('Created table %s', table_name)

    def execute_statement(self, sql_statement):
        """
        Execute a statement in the database.

        Parameters
        ----------
        sql_statement : str
            The statement execute
        """
        # NOTE: The connection does not close after the 'with' statement
        #       Instead we use the context manager as described here
        #       https://stackoverflow.com/a/47501337/2786884
        # Auto-closes connection
        with contextlib.closing(sqlite3.connect(
                str(self.database_path))) as context:
            # Auto-commits
            with context as con:
                # Auto-closes cursor
                with contextlib.closing(con.cursor()) as cur:
                    # Check if tables are present
                    cur.execute(sql_statement)

    def create_parameter_tables(self, parameters_as_sql_types):
        """
        Create a table for each BOUT.settings section and a join table.

        Parameters
        ----------
        parameters_as_sql_types : dict
            The dictionary on the form
            >>> {'section': {'parameter': 'value_type'}}

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        for section in parameters_as_sql_types.keys():
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')
            # Generate foreign keys for the parameters table
            parameters_foreign_keys[f'{section_name}_id'] =\
                (section_name, 'id')

            columns = dict()
            for parameter, value_type in \
                    parameters_as_sql_types[section].items():
                # Generate the columns
                columns[parameter] = value_type

            # Creat the section table
            section_statement = \
                get_create_table_statement(table_name=section_name,
                                           columns=columns)
            self.create_table(section_statement)

        # Create the join table
        parameters_statement = get_create_table_statement(
            table_name='parameters',
            foreign_keys=parameters_foreign_keys)
        self.create_table(parameters_statement)

    def insert_into_parameter_tables(self, parameters_dict):
        """
        Insert the parameters into a the parameter tables.

        Parameters
        ----------
        parameters_dict : dict
            The dictionary on the form
            >>> {'section': {'parameter': 'value'}}

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        for section in parameters_dict.keys():
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')

            # FIXME: YOU ARE HERE
            # FIXME: Check if this combination already exist,
            #  then make an entry
            # About getting the last key
            # https://stackoverflow.com/questions/3442033/sqlite-how-to-get-value-of-auto-increment-primary-key-after-insert-other-than
            # NOTE: About SELECT 1
            # https://stackoverflow.com/questions/7039938/what-does-select-1-from-do
            # NOTE: About checking for existence
            # https://stackoverflow.com/questions/9755860/valid-query-to-check-if-row-exists-in-sqlite3
# SELECT
# 	rowid
# FROM
# 	people
# WHERE
# 	EXISTS(
# 	  SELECT 1
# 	  FROM people
# 	  WHERE first_name="John"
# 	  AND last_name="Doe");

            # FIXME: You didn't start on the stuff below yet

            # Generate foreign keys for the parameters table
            parameters_foreign_keys[f'{section_name}_id'] =\
                (section_name, 'id')

            columns = dict()
            for parameter, value_type in \
                    parameters_dict[section].items():
                # Generate the columns
                columns[parameter] = value_type

            # Creat the section table
            section_statement = \
                get_create_table_statement(table_name=section_name,
                                           columns=columns)
            self.create_table(section_statement)

        # Create the join table
        parameters_statement = get_create_table_statement(
            table_name='parameters',
            foreign_keys=parameters_foreign_keys)
        self.create_table(parameters_statement)

    def capture_data(self, project_path, bout_inp_dir, parameters_dict):
        """
        Capture data from a run.

        Parameters
        ----------
        project_path : Path
            Root path of project (make file)
        bout_inp_dir : Path
            Path to the directory of BOUT.inp currently in use
        parameters_dict : dict of str, dict
            Options on the form
            >>> {'global':{'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}
        """

        # FIXME: Create database entry
        # Parameters
        # Update file_modification
        # System info
        # Run

        parameters_dict = extract_parameters_in_use(project_path,
                                                    bout_inp_dir,
                                                    parameters_dict)

        self.insert_into_parameter_tables(parameters_dict)



        purchases = [('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
                     ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
                     ('2006-04-06', 'SELL', 'IBM', 500, 53.00),
                     ]
        c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)',
                      purchases)

    def update_status(self):
        """Updates the status"""
        raise NotImplementedError('To be implemented')

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

    def insert(self, insert_str):
        """
       Insert to the database.

        Parameters
        ----------
        insert_str : str
            The query to execute
        """
        # Obtain the table name
        pattern = r'INSERT INTO (\w*)'
        table_name = re.match(pattern, insert_str).group(1)

        self.execute_statement(insert_str)

        logging.info('Made insertion to %s', table_name)
