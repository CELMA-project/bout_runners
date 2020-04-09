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
                 database_connector,
                 final_parameters):
        """
        FIXME

        Parameters
        ----------
        FIXME
        """
        # Set member data
        self.__executor = executor
        self.__database_creator = DatabaseCreator(database_connector)
        self.__final_parameters = final_parameters
        self.__metadata_recorder = \
            MetadataRecorder(database_connector,
                             executor.bout_paths,
                             self.__final_parameters)

    def _create_schema(self):
        """Create the schema."""
        final_parameters_dict = \
            self.__final_parameters.get_final_parameters()
        final_parameters_as_sql_types = \
            self.__final_parameters. \
            cast_parameters_to_sql_type(final_parameters_dict)
        self.__database_creator.create_all_schema_tables(
            final_parameters_as_sql_types)

    def run(self):
        """Perform the run and capture data."""
        if not self.__metadata_recorder.database_reader.\
                check_tables_created():
            logging.info('Creating schema as no tables were found in '
                         '%s',
                         self.__metadata_recorder.database_reader.
                         database_connector.database_path)
            self._create_schema()

        self.__metadata_recorder.capture_new_data_from_run(
            self.__executor.submitter.processor_split)

        self.__executor.execute()
