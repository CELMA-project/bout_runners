"""Module containing the StatusChecker class."""


import logging
import psutil
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.log.log_reader import LogReader
from bout_runners.metadata.metadata_updater import MetadataUpdater


class StatusChecker:
    """
    FIXME
    """

    def __init__(self, database_connector, project_path):
        """
        Setup the status checker with a connector, reader and paths

        Notes
        -----
        The StatusChecker instance only checks the project belonging
        to the same database schema grouped together by the
        database_connector

        Parameters
        ----------
        database_connector : DatabaseConnector
            Connection to the database
        project_path : Path
            Path to the project (the root directory with which
            usually contains the makefile and the executable)
        """
        self.__database_connector = database_connector
        self.__database_reader = \
            DatabaseReader(self.__database_connector)
        self.project_path = project_path

    def check_and_update_status(self):
        """Check and update the status for the schema."""
        # Check that run table exist
        if not self.__database_reader.check_tables_created():
            logging.error(
                'No tables found in %s',
                self.__database_reader.database_connector.database_path)
            message = ('Can not check the status of schemas that does '
                       'not exist')
            raise RuntimeError(message)

        # Create place holder metadata_updater
        metadata_updater = \
            MetadataUpdater(self.__database_connector, run_id=-1)

        # Check runs with status 'submitted'
        query = (
            "SELECT name, id AS run_id FROM run WHERE\n"
            "latest_status = 'submitted'")
        submitted_to_check = self.__database_reader.query(query)
        self.__check_submitted(metadata_updater,
                               submitted_to_check)

        # Check runs with status 'running'
        query = (
            "SELECT name FROM run WHERE\n"
            "latest_status = 'running'")
        running_to_check = self.__database_reader.query(query)
        self.__check_running(metadata_updater, running_to_check)

    def __check_submitted(self, metadata_updater, submitted_to_check):
        """
        Check the status of all runs which has status `submitted`.

        Parameters
        ----------
        metadata_updater : MetadataUpdater
            Object which updates the database
        submitted_to_check : DataFrame
            DataFrame containing the `id` and `name` of the runs with
            status `submitted`
        """
        for name, run_id in submitted_to_check.itertuples(index=False):
            metadata_updater.run_id = run_id

            log_path = self.project_path.joinpath(name, 'BOUT.log.0')
            log_reader = LogReader(log_path)

            if log_path.is_file():
                if log_reader.started():
                    start_time = log_reader.start_time
                    metadata_updater.update_start_time(start_time)
                    latest_status = \
                        self.__check_if_stopped(log_reader,
                                                metadata_updater)

                else:
                    # No started time is found in the log
                    latest_status = \
                        self.check_if_running_or_errored(log_reader)
            else:
                # No log file exists
                # NOTE: This means that the execution is either in a
                #       queue or has failed the submission.
                #       For now, we still consider this as submitted
                latest_status = 'submitted'

            metadata_updater.update_latest_status(latest_status)

    def __check_running(self, metadata_updater, running_to_check):
        """
        Check the status of all runs which has status `running`.

        Parameters
        ----------
        metadata_updater : MetadataUpdater
            Object which updates the database
        running_to_check : DataFrame
            DataFrame containing the `id` and `name` of the runs with
            status `running`
        """
        for name, run_id in running_to_check.itertuples(index=False):
            metadata_updater.run_id = run_id
            log_path = self.project_path.joinpath(name, 'BOUT.log.0')
            log_reader = LogReader(log_path)
            latest_status = self.check_if_running_or_errored(log_reader)
            metadata_updater.update_latest_status(latest_status)

    def __check_if_stopped(self, log_reader, metadata_updater):
        """
        Check if a run is still running or has errored.

        Parameters
        ----------
        log_reader : LogReader
            The object which reads log files
        metadata_updater : MetadataUpdater
            Object which updates the database

        Returns
        -------
        latest_status : 'complete' or 'running' or 'error'
            The latest status
        """
        if log_reader.ended():
            end_time = log_reader.end_time
            metadata_updater.update_stop_time(end_time)
            latest_status = 'complete'
        else:
            latest_status = \
                self.check_if_running_or_errored(log_reader)
        return latest_status

    @staticmethod
    def check_if_running_or_errored(log_reader):
        """
        Check if a run is still running or has errored.

        Parameters
        ----------
        log_reader : LogReader
            The object which reads log files

        Returns
        -------
        latest_status : 'running' or 'error'
            The latest status
        """
        pid = log_reader.pid
        if psutil.pid_exists(pid):
            latest_status = 'running'
        else:
            latest_status = 'error'
        return latest_status
