"""Module containing the Bookkeeper class."""


import logging
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_utils import \
    get_file_modification
from bout_runners.database.database_utils import \
    get_system_info
from bout_runners.database.database_utils import \
    extract_parameters_in_use


class Bookkeeper:
    """
    Class for bookkeeping of the runs.

    Attributes
    ----------
    FIXME

    Methods
    -------
    FIXME

    FIXME: Add examples
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database connector
        """
        self.database_connector = database_connector
        # FIXME: Setters and getters?
        self.database_creator = DatabaseCreator(database_connector)
        self.database_writer = DatabaseWriter(database_connector)
        self.database_reader = DatabaseReader(database_connector)

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
            section_id = \
                self.database_reader.get_entry_id(section_name,
                                                  section_parameters)
            if section_id is not None:
                section_id = self.database_writer.create_entry(
                    section_name,
                    section_parameters)

            parameters_foreign_keys[f'{section_name}_id'] = section_id

        # Update the parameters table
        parameters_id = \
            self.database_reader.get_entry_id('parameters',
                                              parameters_foreign_keys)
        if parameters_id is not None:
            parameters_id = self.database_writer.create_entry(
                'parameters',
                parameters_foreign_keys)

        return parameters_id

    # FIXME: YOU ARE HERE. It should be
    #  renamed as the name is already in use in the creator
    def create_parameter_tables(project_path):
        """
        Create one table per section in BOUT.settings and one join table.

        Parameters
        ----------
        project_path : Path
            Path to the project
        """
        settings_path = run_settings_run(project_path,
                                         bout_inp_src_dir=None)
        parameter_dict = obtain_project_parameters(settings_path)
        parameter_dict_as_sql_types = \
            cast_parameters_to_sql_type(parameter_dict)
        database.create_parameter_tables(parameter_dict_as_sql_types)

    # FIXME: This belongs to the object of both runner and bookkeeper
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