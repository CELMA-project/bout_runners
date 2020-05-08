"""Module containing the MetadataUpdater class."""


from bout_runners.database.database_writer import DatabaseWriter


class MetadataUpdater:
    r"""
    Class which updates dynamic data about the run

    Attributes
    ----------
    __database_writer : DatabaseWriter
        Class used to write to the database
    run_id : int
        The id of the run to update

    Methods
    -------
    update_start_time(start_time)
        Update the start time
    update_end_time(end_time)
        Update the end time
    update_latest_status(status)
        Update the latest status

    Examples
    --------
    >>> from pathlib import Path
    >>> from datetime import datetime
    >>> from bout_runners.database.database_connector import \
    ...     DatabaseConnector
    >>> database_root_path = Path().joinpath('path', 'to', 'db_root')
    >>> database_connector = DatabaseConnector('name_of_db',
    ...                                        database_root_path)
    >>> run_id = 1  # This must be a valid id in the run table
    >>> metadata_updater = MetadataUpdater(database_connector, run_id)
    >>> metadata_updater.update_start_time(datetime.now())
    >>> metadata_updater.update_end_time(datetime.now())
    >>> metadata_updater.update_latest_status('error')
    """

    def __init__(self, database_connector, run_id):
        """
        Set the database and id to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database connector
        run_id : int
            The id of the run to update
        """
        self.__database_writer = DatabaseWriter(database_connector)
        self.run_id = run_id

    def update_start_time(self, start_time):
        """
        Update the start time.

        Parameters
        ----------
        start_time : datetime
            The start time of the execution
        """
        self.update_column('start_time', start_time)

    def update_end_time(self, end_time):
        """
        Update the end time.

        Parameters
        ----------
        end_time : datetime
            The end time of the execution
        """
        self.update_column('end_time', end_time)

    def update_latest_status(self, status):
        """
        Update the latest status.

        Parameters
        ----------
        status : str
            The latest status
        """
        self.update_column('latest_status', status)

    def update_column(self, column, value):
        """
        Updates a field with a certain row in the run table.

        Parameters
        ----------
        column : str
            The column to update
        value : object
            The updating value
        """
        update_str = self.__database_writer.create_update_string(
            field_names=(column,),
            table_name='run',
            search_condition=f'id = {self.run_id}'
        )
        self.__database_writer.update(update_str, (value,))
