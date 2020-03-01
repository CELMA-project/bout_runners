"""Module containing the Bookkeeper class."""


import re
import logging
import contextlib
import sqlite3
import pandas as pd
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_file_modification
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info
from bout_runners.bookkeeper.bookkeeper_utils import \
    extract_parameters_in_use


class Bookkeeper:
    """
    Class interacting with the database.

    Attributes
    ----------
    database_path : Path or str
        Path to database
    FIXME: Upate these

    Methods
    -------
    create_table(sql_statement)
        Create a table for each BOUT.settings section and a join table
    query(query_str)
        Make a query to the database
    """

    # FIXME: You are here: Ripping apart Bookkeeper, added from
    #  bookkeeper_utils and creator - remember to move the tests
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

    def execute_statement(self, sql_statement, *parameters):
        """
        Execute a statement in the database.

        Parameters
        ----------
        sql_statement : str
            The statement execute
        parameters : tuple
            Parameters used in .execute of the cursor (like )
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

    def get_db_path(database_root_path, project_path):
        """
        Return the database path.

        Parameters
        ----------
        project_path : None or Path or str
            Root path to the project (i.e. where the makefile is located)
            If None the root caller directory will be used
        database_root_path : None or Path or str
            Root path of the database file
            If None, the path will be set to $HOME/BOUT_db

        Returns
        -------
        database_path : Path
            Path to the database
        """
        if project_path is None:
            project_path = get_caller_dir()

        if database_root_path is None:
            # NOTE: This will be changed when going to production
            database_root_path = get_caller_dir()
            # database_root_path = Path().home().joinpath('BOUT_db')

        project_path = Path(project_path)
        database_root_path = Path(database_root_path)

        database_root_path.mkdir(exist_ok=True, parents=True)
        # NOTE: sqlite does not support schemas (except through an
        #       ephemeral ATTACH connection)
        #       Thus we will make one database per project
        # https://www.sqlite.org/lang_attach.html
        # https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
        schema = project_path.name
        database_path = database_root_path.joinpath(f'{schema}.db')

        return database_path

class BookkeeperWriter:

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
                            runner,
                            number_of_processors,
                            nodes=1,
                            processors_per_node=None):
        """
        Capture data from a run.

        Parameters
        ----------
        runner : BoutRunner
            The bout runner object
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

        # Initiate the run_dict (will be filled with the ids)
        run_dict = {'name': runner.bout_paths.bout_inp_dst_dir.name}

        # Update the parameters
        parameters_dict = \
            extract_parameters_in_use(
                runner.bout_paths.project_path,
                runner.bout_paths.bout_inp_dst_dir,
                runner.run_parameters.run_parameters_dict)
        run_dict['parameters_id'] = \
            self.create_parameter_tables_entry(parameters_dict)

        # Update the file_modification
        file_modification_dict = \
            get_file_modification(runner.bout_paths.project_path,
                                  runner.make.makefile_path,
                                  runner.make.exec_name)
        run_dict['file_modification_id'] = \
            self.check_entry_existence('file_modification',
                                       file_modification_dict)
        if run_dict['file_modification_id'] is None:
            run_dict['file_modification_id'] = \
                self.create_entry('file_modification',
                                  file_modification_dict)

        # Update the split
        split_dict = {'number_of_processors': number_of_processors,
                      'nodes': nodes,
                      'processors_per_nodes': processors_per_node}
        run_dict['split_id'] = \
            self.check_entry_existence('split', split_dict)
        if run_dict['split_id'] is not None:
            run_dict['split_id'] = \
                self.create_entry('split', split_dict)

        # Update the system info
        system_info_dict = get_system_info()
        run_dict['host_id'] = \
            self.check_entry_existence('system_info', system_info_dict)
        if run_dict['host_id'] is not None:
            run_dict['host_id'] = \
                self.create_entry('system_info', system_info_dict)

        # Update the run
        run_id = self.check_entry_existence('run', run_dict)
        if run_id is not None:
            run_dict['latest_status'] = 'submitted'
            self.create_entry('run', run_dict)
            new_entry = True

        return new_entry

    def update_status(self):
        """Update the status."""
        raise NotImplementedError('To be implemented')

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


class BookkeeperReader:

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
        where_statements = list()
        for field, val in entries_dict.items():
            where_statements.append(f'{" "*7}AND {field}="{val}"')
        where_statements[0] = where_statements[0].replace('AND',
                                                          'WHERE')
        where_statements = '\n'.join(where_statements)

        query_str = \
            (f'SELECT rowid\n'
             f'FROM {table_name}\n'
             f'WHERE\n'
             f'    EXISTS(\n'
             f'       SELECT 1\n'
             f'	      FROM {table_name}\n{where_statements})')

        table = self.query(query_str)
        row_id = None if table.empty else table.loc[0, 'rowid']

        return row_id

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

    def tables_created(bookkeeper):
        """
        Check if the tables is created in the database.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            The Bookkeeper object

        Returns
        -------
        bool
            Whether or not the tables are created
        """
        query_str = ('SELECT name FROM sqlite_master '
                     '   WHERE type="table"')

        table = bookkeeper.query(query_str)
        return len(table.index) != 0


class BookkeeperCreator:

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

    def get_create_table_statement(table_name,
                                   columns=None,
                                   primary_key='id',
                                   foreign_keys=None):
        """
        Return a SQL string which can be used to create the table.

        Parameters
        ----------
        table_name : str
            Name of the table
        columns : dict or None
            Dictionary where the key is the column name and the value is
            the type
        primary_key : str
            Name of the primary key (the type is set to INTEGER)
        foreign_keys : dict or None
            Dictionary where the key is the column in this table to be
            used as a foreign key and the value is the tuple
            consisting of (name_of_the_table, key_in_table) to refer to

        Returns
        -------
        create_statement : str
            The SQL statement which creates table
        """
        create_statement = f'CREATE TABLE {table_name} \n('

        create_statement += f'   {primary_key} INTEGER PRIMARY KEY,\n'

        # These are not known during submission time
        nullable_fields = ('start_time',
                           'stop_time')
        if columns is not None:
            for name, sql_type in columns.items():
                create_statement += f'    {name} {sql_type}'
                if name in nullable_fields:
                    nullable_str = ',\n'
                else:
                    nullable_str = ' NOT NULL,\n'
                create_statement += nullable_str

        if foreign_keys is not None:
            # Create the key as column
            # NOTE: All keys are integers
            for name in foreign_keys.keys():
                create_statement += f'    {name} INTEGER NOT NULL,\n'

            # Add constraint
            for name, (f_table_name, key_in_table) in foreign_keys.items():
                # If the parent is updated or deleted, we would like the
                # same effect to apply to its child (thereby the CASCADE
                # parameter)
                create_statement += \
                    (f'    FOREIGN KEY({name}) \n'
                     f'        REFERENCES {f_table_name}({key_in_table})\n'
                     f'            ON UPDATE CASCADE\n'
                     f'            ON DELETE CASCADE,'
                     f'\n')

        # Replace last comma with )
        create_statement = f'{create_statement[:-2]})'

        return create_statement