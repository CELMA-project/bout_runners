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

/*Get all the tables*/
SELECT name FROM sqlite_master
WHERE
    type ='table' AND
    name NOT LIKE 'sqlite_%';

/*For each table*/
SELECT name FROM pragma_table_info('parameters');

/*Extract all with x_id from all tables...the x contains the table to join*/
/*First join the parameters to one table as a subquery*/
/*Join the rest to a normal inner join*/
SELECT * FROM parameters
INNER JOIN conduction ON parameters.conduction_id = conduction.id
INNER JOIN all_boundaries ON parameters.all_boundaries_id = all_boundaries.id;