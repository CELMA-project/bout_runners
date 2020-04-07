"""Contains the BOUT runner class."""


import logging
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.metadata_recorder.metadata_recorder import \
    MetadataRecorder


# FIXME: You are here
class BoutRunner:
    """
    Executes the command for submitting a run.

    FIXME: Add variables and attributes

    FIXME: Add examples
    """

    def __init__(self,
                 executor,
                 database_connector):
        """
        FIXME

        Parameters
        ----------
        FIXME
        """
        # Set member data
        self.__executor = executor
        self.__database_creator = DatabaseCreator(database_connector)
        self.__metadata_recorder = \
            MetadataRecorder(database_connector,
                             executor.bout_paths,
                             final_parameters)

    def create_schema(self):
        """
        Create the schema.

        The schema is created by executing a settings run in order to
        infer the parameters of the project executable. The
        parameters are subsequently read and their types cast to
        SQL types
        """
        settings_path = self.run_settings_run()
        parameter_dict = self.obtain_project_parameters(settings_path)
        parameter_dict_as_sql_types = \
            self.cast_parameters_to_sql_type(parameter_dict)
        self.__database_creator.create_all_schema_tables(
            parameter_dict_as_sql_types)

    def run(self):
        """Perform the run and capture data."""
        if not self.__metadata_recorder.database_reader.\
                check_tables_created():
            logging.info('Creating schema as no tables were found in '
                         '%s',
                         self.__metadata_recorder.database_reader.
                         database_connector.database_path)
            self.create_schema()

        self.__metadata_recorder.capture_new_data_from_run(
            self.__executor.processor_split)

        self.__executor.execute()
