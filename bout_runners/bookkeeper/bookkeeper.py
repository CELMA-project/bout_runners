import logging
import contextlib
import sqlite3
import pandas as pd
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement

# FIXME: Make a sql object which contains query, insert, write etc
#  with member data database_path. Should instance hold connection
#  open? Probably not to avoid concurrency problems


class Bookkeeper(object):
    """
    Class dealing with the database.

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
        logging.info(f'Database path set to {self.database_path}')

    def create_table(self, sql_statement):
        """
        Creates a table in the database

        Parameters
        ----------
        sql_statement : str
            The query to execute
        """
        # NOTE: The connection does not close after the 'with' statement
        #       Instead we use the context manager as described here
        #       https://stackoverflow.com/a/47501337/2786884
        # Auto-closes connection
        with contextlib.closing(sqlite3.connect(
                str(self.database_path))) as con:
            # Auto-commits
            with con as c:
                # Auto-closes cursor
                with contextlib.closing(c.cursor()) as cur:
                    # Check if tables are present
                    cur.execute(sql_statement)

    def create_parameter_tables(self, parameter_dict):
        """
        Create a table for each BOUT.settings section and a join table.

        Parameters
        ----------
        parameter_dict : dict
            The dictionary on the same form as the output of
            FIXME: Update Path
            bout_runners.obtain_project_parameters

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        for section in parameter_dict.keys():
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')
            # Generate foreign keys for the parameters table
            parameters_foreign_keys[f'{section_name}_id'] =\
                (section_name, 'id')

            columns = dict()
            for parameter, value_type in \
                    parameter_dict[section].items():
                # Generate the columns
                columns[parameter] = value_type

            # Creat the section table
            section_statement = \
                get_create_table_statement(name=section_name,
                                           columns=columns)
            self.create_table(section_statement)

        # Create the join table
        parameters_statement = get_create_table_statement(
            name='parameters', foreign_keys=parameters_foreign_keys)
        self.create_table(parameters_statement)

    def query(self, query_str):
        """
        Makes a query to the database

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
            df = pd.read_sql_query(query_str, con)
        return df
