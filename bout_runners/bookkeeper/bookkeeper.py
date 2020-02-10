"""Module containing the Bookkeeper class."""


import re
import logging
import contextlib
import sqlite3
import numpy as np
import pandas as pd
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    create_insert_string
from bout_runners.bookkeeper.bookkeeper_utils import \
    extract_parameters_in_use
from bout_runners.runners.runner_utils import get_file_modification
from bout_runners.runners.runner_utils import get_system_info


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

    def execute_statement(self, sql_statement, *parameters):
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
                    cur.execute(sql_statement, parameters)

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

    def create_parameter_tables_entry(self, parameters_dict):
        """
        Insert the parameters into a the parameter tables.

        Parameters
        ----------
        parameters_dict : dict
            The dictionary on the form
            >>> {'section': {'parameter': 'value'}}

        Returns
        -------
        parameters_id : int
            The id key from the `parameters` table

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        parameter_sections = list(parameters_dict.keys())

        for section in parameter_sections:
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')
            section_parameters = parameters_dict[section]
            section_id = self.check_entry_existence(section_name,
                                                    section_parameters)
            if section_id is not None:
                section_id = self.create_entry(section_name,
                                               section_parameters)

            parameters_foreign_keys[f'{section_name}_id'] = section_id

        # Update the parameters table
        parameters_id = \
            self.check_entry_existence('parameters',
                                       parameters_foreign_keys)
        if parameters_id is not None:
            parameters_id = self.create_entry('parameters',
                                              parameters_foreign_keys)

        return parameters_id

    def create_entry(self, table_name, entries_dict):
        """
        Create a database entry.

        Parameters
        ----------
        table_name : str
            Name of the table
        entries_dict : dict
            Dictionary containing the entries as key value pairs

        Returns
        -------
        latest_row_id : int
            The id of the entry just made
        """
        keys = entries_dict.keys()
        values = entries_dict.values()
        insert_str = create_insert_string(keys, table_name)
        self.insert(insert_str, values)

        latest_row_id = self.get_latest_row_id()

        return latest_row_id

    def store_data_from_run(self,
                            run_name,
                            project_path,
                            bout_inp_dir,
                            makefile_path,
                            exec_name,
                            parameters_dict,
                            number_of_processors,
                            nodes=1,
                            processors_per_node=None):
        """
        Capture data from a run.

        Parameters
        ----------
        run_name : str
            Name of the run
        project_path : Path
            Root path of project (make file)
        bout_inp_dir : Path
            Path to the directory of BOUT.inp currently in use
        makefile_path : Path
            Path to the project makefile
        exec_name : str
            Name of the executable
        parameters_dict : dict of str, dict
            Options on the form
            >>> {'global':{'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}
        number_of_processors : int
            The total number of processors
        nodes : int
            The total number of nodes used
        processors_per_node : int
            Number of processors per nodes.
            If None, this will be set to
            floor(number_of_processors/nodes)

        Returns
        -------
        new_entry : bool
            Returns True if this a new entry is made, False if not
        """
        new_entry = False

        # Update the parameters
        parameters_dict = extract_parameters_in_use(project_path,
                                                    bout_inp_dir,
                                                    parameters_dict)
        parameters_id = \
            self.create_parameter_tables_entry(parameters_dict)

        # Update the file_modification
        file_modification_dict = \
            get_file_modification(project_path,
                                  makefile_path,
                                  exec_name)
        file_modification_id = \
            self.check_entry_existence('file_modification',
                                       file_modification_dict)
        if file_modification_id is None:
            file_modification_id = \
                self.create_entry('file_modification',
                                  file_modification_dict)

        # Update the split
        processors_per_node = processors_per_node if \
            processors_per_node is not None else \
            int(np.floor(number_of_processors/nodes))
        split_dict = {'number_of_processors': number_of_processors,
                      'nodes': nodes,
                      'processors_per_nodes': processors_per_node}
        split_id = self.check_entry_existence('split', split_dict)
        if split_id is not None:
            split_id = self.create_entry('split', split_dict)

        # Update the system info
        system_info_dict = get_system_info()
        system_info_id = self.check_entry_existence('system_info',
                                                    system_info_dict)
        if system_info_id is not None:
            system_info_id = \
                self.create_entry('system_info', system_info_dict)

        # Update the run
        run_dict = {'name': run_name,
                    'latest_status': 'submitted',
                    'file_modification_id': file_modification_id,
                    'split_id': split_id,
                    'parameters_id': parameters_id,
                    'host_id': system_info_id}
        run_id = self.check_entry_existence('run', run_dict)
        if run_id is not None:
            self.create_entry('run', run_dict)
            new_entry = True

        return new_entry

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

    def check_entry_existence(self, table_name, entries_dict):
        """
        Check if the entry already exists in the table entry.

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
            The id of the hit. If none is found, None is returned
        """
        # NOTE: About checking for existence
        # https://stackoverflow.com/questions/9755860/valid-query-to-check-if-row-exists-in-sqlite3
        # NOTE: About SELECT 1
        # https://stackoverflow.com/questions/7039938/what-does-select-1-from-do
        query_str = \
            ('SELECT rowid\n'
             'FROM people\n'
             'WHERE\n'
             '    EXISTS(\n'
             '        SELECT 1\n'
             '	      FROM people\n'
             '	      WHERE first_name="John"\n'
             '	      AND last_name="Doe")')

        df = self.query(query_str)
        row_id = None if df.empty else df.loc[0, 'rowid']

        return row_id

    def update_status(self):
        """Update the status."""
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

        self.execute_statement(insert_str, values)

        logging.info('Made insertion to %s', table_name)
