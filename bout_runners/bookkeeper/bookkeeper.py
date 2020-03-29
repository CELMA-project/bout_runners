"""Module containing the Bookkeeper class."""


from bout_runners.make.make import MakeProject
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_utils import \
    get_file_modification
from bout_runners.database.database_utils import \
    get_system_info


class Bookkeeper:
    """
    Class for bookkeeping of the runs.

    Attributes
    ----------
    __database_writer : DatabaseWriter
        Getter and setter variable for database_writer
    __database_reader : DatabaseReader
        Getter and setter variable for database_reader
    database_writer : DatabaseWriter
        Object which writes to the database
    database_reader : DatabaseReader
        Object which reads from the database

    Methods
    -------
    capture_new_data_from_run(runner, processor_split)
        Capture new data from a run
    _create_parameter_tables_entry(parameters_dict)
        Insert the parameters into a the parameter tables

    FIXME: Add examples
    """

    def __init__(self,
                 database_connector,
                 bout_paths,
                 final_parameters):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database connector
        bout_paths : BoutPaths
            Object containing the paths
        final_parameters : FinalParameters
            Object containing the final parameters
        """
        self.__database_writer = DatabaseWriter(database_connector)
        self.__database_reader = DatabaseReader(database_connector)
        self.__bout_paths = bout_paths
        self.__final_parameters = final_parameters
        self.__make = MakeProject(self.__bout_paths.project_path)

    @property
    def database_reader(self):
        """
        Set the properties of self.database_reader.

        Returns
        -------
        self.__database_reader : DatabaseReader
            The database reader object

        Notes
        -----
        The database_reader is read only
        """
        return self.__database_reader

    @database_reader.setter
    def database_reader(self, _):
        msg = (f'The database_reader is read only, and is '
               f'set through the constructor')
        raise AttributeError(msg)

    @property
    def database_writer(self):
        """
        Set the properties of self.database_writer.

        Returns
        -------
        self.__database_writer : DatabaseWriter
            The database writer object

        Notes
        -----
        The database_writer is read only
        """
        return self.__database_writer

    @database_writer.setter
    def database_writer(self, _):
        msg = (f'The database_writer is read only, and is '
               f'set through the constructor')
        raise AttributeError(msg)

    def capture_new_data_from_run(self, processor_split):
        """
        Capture new data from a run.

        This function will capture all uncaptured data from a run.
        If all data has been captured previously, it means that the
        run has already been executed, and new_entry = False will be
        returned.

        Parameters
        ----------
        processor_split : ProcessorSplit
            The processor split object

        Returns
        -------
        new_entry : bool
            Returns True if this a new entry is made, False if not
        """
        new_entry = False

        # Initiate the run_dict (will be filled with the ids)
        run_dict = {'name': self.__bout_paths.bout_inp_dst_dir.name}

        # Update the parameters
        parameters_dict = self.__final_parameters.get_final_parameters()

        run_dict['parameters_id'] = \
            self._create_parameter_tables_entry(parameters_dict)

        # Update the file_modification
        file_modification_dict = \
            get_file_modification(self.__bout_paths.project_path,
                                  self.__make.makefile_path,
                                  self.__make.exec_name)
        run_dict['file_modification_id'] = \
            self.__database_reader.get_entry_id('file_modification',
                                                file_modification_dict)
        if run_dict['file_modification_id'] is None:
            run_dict['file_modification_id'] = \
                self.__database_writer.create_entry(
                    'file_modification',
                    file_modification_dict)

        # Update the split
        split_dict = {'number_of_processors':
                      processor_split.number_of_processors,
                      'nodes': processor_split.nodes,
                      'processors_per_node':
                      processor_split.processors_per_node}
        run_dict['split_id'] = \
            self.__database_reader.get_entry_id('split', split_dict)
        if run_dict['split_id'] is not None:
            run_dict['split_id'] = \
                self.__database_writer.create_entry('split', split_dict)

        # Update the system info
        system_info_dict = get_system_info()
        run_dict['host_id'] = self.__database_reader.get_entry_id(
            'system_info', system_info_dict)
        if run_dict['host_id'] is not None:
            run_dict['host_id'] = \
                self.__database_writer.create_entry('system_info',
                                                    system_info_dict)

        # Update the run
        run_id = self.__database_reader.get_entry_id('run', run_dict)
        if run_id is not None:
            run_dict['latest_status'] = 'submitted'
            self.__database_writer.create_entry('run', run_dict)
            new_entry = True

        return new_entry

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
                self.__database_reader.get_entry_id(section_name,
                                                    section_parameters)
            if section_id is not None:
                section_id = self.__database_writer.create_entry(
                    section_name,
                    section_parameters)

            parameters_foreign_keys[f'{section_name}_id'] = section_id

        # Update the parameters table
        parameters_id = \
            self.__database_reader.get_entry_id('parameters',
                                                parameters_foreign_keys)
        if parameters_id is not None:
            parameters_id = self.__database_writer.create_entry(
                'parameters',
                parameters_foreign_keys)

        return parameters_id
