"""Module containing the BookkeeperCreator class."""

# FIXME: You are here: Ripping apart Bookkeeper, added from
#  bookkeeper_utils and creator - remember to move the tests
# FIXME: Seems like no need to make abstract classes (except for
#  runner)

import re
import logging
from bout_runners.bookkeeper.bookkeeper_base import BookkeeperBase
from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    cast_parameters_to_sql_type
from bout_runners.bookkeeper.bookkeeper_utils import get_db_path
from bout_runners.bookkeeper.bookkeeper_utils import tables_created
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper_base import Bookkeeper
from bout_runners.runners.runner_utils import run_settings_run


class BookkeeperCreator(BookkeeperBase):
    """
    Class for creating the schema of the database.

    Attributes
    ----------
    FIXME

    Methods
    -------
    FIXME

    FIXME: Add examples
    """

    def __init__(self, name=None, database_root_path=None):
        """
        Call the parent class.

        Parameters
        ----------
        name : str or None
            Name of the database (excluding .db)
            If set to None, the name of the caller directory will be
            used
        database_root_path : Path or str or None
            Path to database
            If None is set, the path will be set to $HOME/BOUT_db
        """
        super().__init__(name, database_root_path)

    def create_tables(self):
        """
        Create the database if it doesn't already exist.

        The database will be on normalized form, see [1]_ for a quick
        overview, and [2]_ for a slightly deeper explanation

        References
        ----------
        [1] https://www.databasestar.com/database-normalization/
        [2] http://www.bkent.net/Doc/simple5.htm
        """
        logging.info(f'Creating tables in {self.database_path}')

        # Check if tables are created
        # FIXME: Here a reading will take place, should that be done
        #  here or in the reader?
        if tables_created(bookkeeper):
            create_system_info_table(bookkeeper)
            create_split_table(bookkeeper)
            create_file_modification_table(bookkeeper)
            create_parameter_tables(bookkeeper, project_path)
            create_run_table(bookkeeper)

        logging.info(f'Tables created in {self.database_path}')

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



    def create_run_table(bookkeeper):
        """
        Create a table for the metadata of a run.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            A bookkeeper object which doe the sql calls
        """
        run_statement = \
            get_create_table_statement(
                table_name='run',
                columns={'name': 'TEXT',
                         'submitted_time': 'TIMESTAMP',
                         'start_time': 'TIMESTAMP',
                         'stop_time': 'TIMESTAMP',
                         'latest_status': 'TEXT',
                         },
                foreign_keys={'file_modification_id':
                                  ('file_modification', 'id'),
                              'split_id': ('split', 'id'),
                              'parameters_id': ('parameters', 'id'),
                              'host_id': ('host', 'id')})
        bookkeeper.create_table(run_statement)

    def create_parameter_tables(bookkeeper, project_path):
        """
        Create one table per section in BOUT.settings and one join table.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            A bookkeeper object which doe the sql calls
        project_path : Path
            Path to the project
        """
        settings_path = run_settings_run(project_path,
                                         bout_inp_src_dir=None)
        parameter_dict = obtain_project_parameters(settings_path)
        parameter_dict_sql_types = \
            cast_parameters_to_sql_type(parameter_dict)
        bookkeeper.create_parameter_tables(parameter_dict_sql_types)

    def create_file_modification_table(bookkeeper):
        """
        Create a table for file modifications.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            A bookkeeper object which doe the sql calls
        """
        file_modification_statement = \
            get_create_table_statement(
                table_name='file_modification',
                columns={'project_makefile_modified': 'TIMESTAMP',
                         'project_executable_modified': 'TIMESTAMP',
                         'project_git_sha': 'TEXT',
                         'bout_lib_modified': 'TIMESTAMP',
                         'bout_git_sha': 'TEXT'})
        bookkeeper.create_table(file_modification_statement)

    def create_split_table(bookkeeper):
        """
        Create a table which stores the grid split.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            A bookkeeper object which doe the sql calls
        """
        split_statement = \
            get_create_table_statement(
                table_name='split',
                columns={'number_of_processors': 'INTEGER',
                         'nodes': 'INTEGER',
                         'processors_per_node': 'INTEGER'})
        bookkeeper.create_table(split_statement)

    def create_system_info_table(bookkeeper):
        """
        Create a table for the system info.

        Parameters
        ----------
        bookkeeper : Bookkeeper
            A bookkeeper object which doe the sql calls
        """
        sys_info_dict = get_system_info_as_sql_type()
        sys_info_statement = \
            get_create_table_statement(table_name='system_info',
                                       columns=sys_info_dict)
        bookkeeper.create_table(sys_info_statement)




