"""Module containing the MetadataReader class."""


from datetime import datetime
from bout_runners.make.make import Make
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_utils import \
    get_file_modification
from bout_runners.database.database_utils import \
    get_system_info


class MetadataReader:
    """
    Class for reading the metadata from the database.

    FIXME
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        FIXME
        """
        self.__database_reader = DatabaseReader(database_connector)

    def get_all_metadata(self):
        """
        FIXME
        """
        pass

    def get_parameters_metadata(self, table_name, entries_dict):
        """
        FIXME
        """
