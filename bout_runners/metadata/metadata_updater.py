"""Module containing the MetadataUpdater class."""


from bout_runners.database.database_writer import DatabaseWriter


class MetadataUpdater:
    """
    FIXME
    """
    # FIXME: YOU ARE HERE: MAKE TESTS

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database connector
        """
        self.__database_writer = DatabaseWriter(database_connector)

    def update_start_time(self, start_time, run_id):
        """
        Updates the start time.

        Parameters
        ----------
        start_time : datetime
            The start time of the execution
        run_id : int
            The id of the run to update
        """
        self.update_column_with_run_id('start_time', start_time, run_id)

    def update_end_time(self, end_time, run_id):
        """
        Updates the end time.

        Parameters
        ----------
        end_time : datetime
            The end time of the execution
        run_id : int
            The id of the run to update
        """
        self.update_column_with_run_id('end_time', end_time, run_id)

    def update_latest_status(self, status, run_id):
        """
        Updates the latest status.

        Parameters
        ----------
        status : str
            The latest status
        run_id : int
            The id of the run to update
        """
        self.update_column_with_run_id('latest_status', status, run_id)

    def update_column_with_run_id(self, column, value, run_id):
        """
        Updates a field with a certain row in the run table.

        Parameters
        ----------
        column : str
            The column to update
        value : object
            The updating value
        run_id : int
            The id of the run to update
        """
        update_str = self.__database_writer.create_update_string(
            field_names=(column,),
            table_name='run',
            search_condition=f'id = {run_id}'
        )
        self.__database_writer.update(update_str, (value,))
