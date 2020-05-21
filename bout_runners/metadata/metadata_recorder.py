"""Module containing the MetadataRecorder class."""


from datetime import datetime
from bout_runners.make.make import Make
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_utils import get_file_modification
from bout_runners.database.database_utils import get_system_info


class MetadataRecorder:
    r"""
    Class for recording the metadata of the runs.

    Attributes
    ----------
    __db_writer : DatabaseWriter
        Getter variable for db_writer
    __db_reader : DatabaseReader
        Getter variable for db_reader
    db_writer : DatabaseWriter
        Object which writes to the database
    db_reader : DatabaseReader
        Object which reads from the database

    Methods
    -------
    capture_new_data_from_run(runner, processor_split)
        Capture new data from a run
    _create_parameter_tables_entry(parameters_dict)
        Insert the parameters into a the parameter tables

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
    >>> from bout_runners.submitter.processor_split import \
    ...     ProcessorSplit

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
    ...     final_parameters.cast_to_sql_type(
    ...     final_parameters_dict)

    Create the metadata recorder object
    >>> db_connection = DatabaseConnector('name')
    >>> metadata_recorder = MetadataRecorder(db_connection,
    ...                                      bout_paths,
    ...                                      final_parameters)

    Capture the data to the database
    >>> metadata_recorder.capture_new_data_from_run(ProcessorSplit())
    None
    """

    def __init__(self, db_connector, bout_paths, final_parameters):
        """
        Set the database to use.

        Parameters
        ----------
        db_connector : DatabaseConnector
            The database connector
        bout_paths : BoutPaths
            Object containing the paths
        final_parameters : FinalParameters
            Object containing the final parameters
        """
        self.__db_writer = DatabaseWriter(db_connector)
        self.__db_reader = DatabaseReader(db_connector)
        self.__bout_paths = bout_paths
        self.__final_parameters = final_parameters
        self.__make = Make(self.__bout_paths.project_path)

    @property
    def db_reader(self):
        """
        Set the properties of self.db_reader.

        Returns
        -------
        self.__db_reader : DatabaseReader
            The database reader object

        Notes
        -----
        The db_reader is read only
        """
        return self.__db_reader

    @property
    def db_writer(self):
        """
        Set the properties of self.db_writer.

        Returns
        -------
        self.__db_writer : DatabaseWriter
            The database writer object

        Notes
        -----
        The db_writer is read only
        """
        return self.__db_writer

    def capture_new_data_from_run(self, processor_split, force=False):
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
        force : bool
            Store entry to the run table even if a entry with the
            same parameter exists
            This will typically be used if the bout_runners is
            forcefully executing a run

        Returns
        -------
        run_id : None or int
            If no previous run with the same configuration has been
            executed, this will return None, else the run_id is returned
        """
        # Initiate the run_dict (will be filled with the ids)
        run_dict = {"name": self.__bout_paths.bout_inp_dst_dir.name}

        # Update the parameters
        parameters_dict = self.__final_parameters.get_final_parameters()

        run_dict["parameters_id"] = self._create_parameter_tables_entry(parameters_dict)

        # Update the file_modification
        file_modification_dict = get_file_modification(
            self.__bout_paths.project_path,
            self.__make.makefile_path,
            self.__make.exec_name,
        )
        run_dict["file_modification_id"] = self.__db_reader.get_entry_id(
            "file_modification", file_modification_dict
        )
        if run_dict["file_modification_id"] is None:
            run_dict["file_modification_id"] = self.create_entry(
                "file_modification", file_modification_dict
            )

        # Update the split
        split_dict = {
            "number_of_processors": processor_split.number_of_processors,
            "number_of_nodes": processor_split.number_of_nodes,
            "processors_per_node": processor_split.processors_per_node,
        }
        run_dict["split_id"] = self.__db_reader.get_entry_id("split", split_dict)
        if run_dict["split_id"] is None:
            run_dict["split_id"] = self.create_entry("split", split_dict)

        # Update the system info
        system_info_dict = get_system_info()
        run_dict["system_info_id"] = self.__db_reader.get_entry_id(
            "system_info", system_info_dict
        )
        if run_dict["system_info_id"] is None:
            run_dict["system_info_id"] = self.create_entry(
                "system_info", system_info_dict
            )

        # Update the run
        run_id = self.__db_reader.get_entry_id("run", run_dict)
        if force or run_id is None:
            run_dict["latest_status"] = "submitted"
            run_dict["submitted_time"] = datetime.now().isoformat()
            _ = self.create_entry("run", run_dict)

        return run_id

    def create_entry(self, table_name, entries_dict):
        """
        Create a database entry and return the entry id.

        Parameters
        ----------
        table_name : str
            Name of the table
        entries_dict : dict
            Dictionary containing the entries as key value pairs

        Returns
        -------
        entry_id : int
            The id of the newly created entry
        """
        self.__db_writer.create_entry(table_name, entries_dict)
        entry_id = self.__db_reader.get_entry_id(table_name, entries_dict)
        return entry_id

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
            section_name = section.replace(":", "_")
            section_parameters = parameters_dict[section]
            section_id = self.__db_reader.get_entry_id(section_name, section_parameters)
            if section_id is None:
                section_id = self.create_entry(section_name, section_parameters)

            parameters_foreign_keys[f"{section_name}_id"] = section_id

        # Update the parameters table
        parameters_id = self.__db_reader.get_entry_id(
            "parameters", parameters_foreign_keys
        )
        if parameters_id is None:
            parameters_id = self.create_entry("parameters", parameters_foreign_keys)

        return parameters_id
