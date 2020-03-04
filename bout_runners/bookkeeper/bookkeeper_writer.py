"""Module containing the BookkeeperWriter class."""


import re
import logging
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_file_modification
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info
from bout_runners.bookkeeper.bookkeeper_utils import \
    extract_parameters_in_use


class BookkeeperWriter:
    """
    Class for writing to the schema of the database.

    Attributes
    ----------
    FIXME

    Methods
    -------
    FIXME

    FIXME: Add examples
    """

    def __init__(self, bookkeeper):
        """
        Set the bookkeeper to use.

        Parameters
        ----------
        bookkeeper : BookkeeperConnector
            The bookkeeper object to write to
        """
        self.bookkeeper = bookkeeper

    @staticmethod
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

        self.bookkeeper.execute_statement(insert_str, values)

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
        """
        keys = entries_dict.keys()
        values = entries_dict.values()
        insert_str = self.create_insert_string(keys, table_name)
        self.insert(insert_str, values)

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
            self._create_parameter_tables_entry(parameters_dict)

        # Update the file_modification
        file_modification_dict = \
            get_file_modification(runner.bout_paths.project_path,
                                  runner.make.makefile_path,
                                  runner.make.exec_name)
        # FIXME: YOU ARE HERE: WE ARE HERE MIXING RESPONSIBILITIES
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

    def _create_parameter_tables_entry(self, parameters_dict):
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
