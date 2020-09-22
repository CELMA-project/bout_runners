"""Module containing the StatusChecker class."""


import logging
import time
from typing import Optional, Union
from pathlib import Path

import psutil
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.log.log_reader import LogReader
from bout_runners.metadata.metadata_updater import MetadataUpdater
from pandas import DataFrame


class StatusChecker:
    r"""
    Class to check and update the status of runs.

    Attributes
    ----------
    __db_connector : DatabaseConnector
        Connection to the database under consideration
    __db_reader : DatabaseReader
        Object to read the database with
    project_path : Path
        Path to the project

    Methods
    -------
    check_and_update_status()
        Check and update the status for the schema
    check_and_update_status_until_complete()
        Check and update the status until all runs are stopped
    __check_submitted(metadata_updater, submitted_to_check)
        Check the status of all runs which has status `submitted`
    __check_running(metadata_updater, running_to_check)
        Check the status of all runs which has status `running`
     __check_if_stopped(log_reader, metadata_updater)
        Check if a run has stopped
    check_if_running_or_errored(log_reader)
        Check if a run is still running or has errored

    Examples
    --------
    >>> from pathlib import Path
    >>> from bout_runners.database.database_connector import \
    ...     DatabaseConnector
    >>> db_connector = DatabaseConnector('name_of_db')
    >>> project_path = Path('path').joinpath('to', 'project')
    >>> status_checker = StatusChecker(db_connector, project_path)
    >>> status_checker.check_and_update_status()

    Any updates to the runs will be written to the database.
    Alternatively, one can run the program until all jobs have
    stopped by calling

    >>> status_checker.check_and_update_until_complete()
    """

    def __init__(
        self,
        db_connector: Optional[DatabaseConnector] = None,
        project_path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Set connector, reader and a project path.

        Notes
        -----
        The StatusChecker instance only checks the project belonging to the same
        database schema grouped together by the `db_connector`

        Parameters
        ----------
        db_connector : DatabaseConnector
            Connection to the database
        project_path : Path
            Path to the project (the root directory with which usually contains the
            makefile and the executable)
        """
        self.__db_connector = (
            db_connector if db_connector is not None else DatabaseConnector()
        )
        self.__db_reader = DatabaseReader(self.__db_connector)
        self.__project_path = Path(project_path) if project_path is not None else Path()

    def check_and_update_status(self) -> None:
        """
        Check and update the status for the schema.

        Raises
        ------
        RuntimeError
            If the schema does not exist
        """
        # Check that run table exist
        if not self.__db_reader.check_tables_created():
            logging.error(
                "No tables found in %s",
                self.__db_reader.db_connector.db_path,
            )
            message = "Can not check the status of schemas that does not exist"
            raise RuntimeError(message)

        # Create place holder metadata_updater
        metadata_updater = MetadataUpdater(self.__db_connector, run_id=-1)

        # Check runs with status 'submitted'
        query = (
            "SELECT name, id AS run_id FROM run WHERE\n"
            "latest_status = 'submitted' OR\n"
            "latest_status = 'created'"
        )
        submitted_to_check = self.__db_reader.query(query)
        self.__check_submitted(metadata_updater, submitted_to_check)

        # Check runs with status 'running'
        query = 'SELECT name, id FROM run WHERE latest_status = "running"'
        running_to_check = self.__db_reader.query(query)
        self.__check_running(metadata_updater, running_to_check)

    @staticmethod
    def get_query_string_for_non_errored_runs() -> str:
        """
        Return the query string for non errored results.

        Returns
        -------
        str
            Query string for non errored results
        """
        return (
            "SELECT name, id AS run_id FROM run WHERE\n"
            "latest_status = 'submitted' OR\n"
            "latest_status = 'created' OR\n"
            "latest_status = 'running'"
        )

    def check_and_update_until_complete(self, seconds_between_update: int = 5) -> None:
        """
        Check and update the status until all runs are stopped.

        Parameters
        ----------
        seconds_between_update : int
            Number of seconds before a new status check is performed
        """
        query = self.get_query_string_for_non_errored_runs()
        while len(self.__db_reader.query(query).index) != 0:
            self.check_and_update_status()
            time.sleep(seconds_between_update)

    def __check_submitted(
        self, metadata_updater: MetadataUpdater, submitted_to_check: DataFrame
    ) -> None:
        """
        Check the status of all runs which has status `submitted`.

        Parameters
        ----------
        metadata_updater : MetadataUpdater
            Object which updates the database
        submitted_to_check : DataFrame
            DataFrame containing the `id` and `name` of the runs with status `submitted`

        Raises
        ------
        RuntimeError
            In case log_reader.started() is True and log_reader.start_time is None
        """
        for name, run_id in submitted_to_check.itertuples(index=False):
            metadata_updater.run_id = run_id

            log_path = self.__project_path.joinpath(name, "BOUT.log.0")

            if log_path.is_file():
                log_reader = LogReader(log_path)
                if log_reader.started():
                    start_time = log_reader.start_time
                    # Assert to prevent "Incompatible types in assignment" with Optional
                    if start_time is None:
                        raise RuntimeError(
                            "log_reader.start_time is None although "
                            "log_reader.started is True"
                        )
                    metadata_updater.update_start_time(start_time)
                    latest_status = self.__check_if_stopped(
                        log_reader, metadata_updater
                    )

                else:
                    # No started time is found in the log
                    latest_status = self.check_if_running_or_errored(log_reader)
            else:
                # No log file exists
                # FIXME: Check pid
                # NOTE: This means that the execution is either in a
                #       queue or has failed the submission.
                #       For now, we still consider this as submitted
                latest_status = "submitted"

            metadata_updater.update_latest_status(latest_status)

    def __check_running(
        self, metadata_updater: MetadataUpdater, running_to_check: DataFrame
    ) -> None:
        """
        Check the status of all runs which has status `running`.

        Parameters
        ----------
        metadata_updater : MetadataUpdater
            Object which updates the database
        running_to_check : DataFrame
            DataFrame containing the `id` and `name` of the runs with status `running`
        """
        for name, run_id in running_to_check.itertuples(index=False):
            metadata_updater.run_id = run_id
            log_path = self.__project_path.joinpath(name, "BOUT.log.0")
            log_reader = LogReader(log_path)
            latest_status = self.check_if_running_or_errored(log_reader)
            metadata_updater.update_latest_status(latest_status)

    def __check_if_stopped(
        self, log_reader: LogReader, metadata_updater: MetadataUpdater
    ) -> str:
        """
        Check if a run has stopped.

        Parameters
        ----------
        log_reader : LogReader
            The object which reads log files
        metadata_updater : MetadataUpdater
            Object which updates the database

        Returns
        -------
        latest_status : str
            The latest status

        Raises
        ------
        RuntimeError
            In case log_reader.ended() is True and log_reader.end_time is None
        """
        if log_reader.ended():
            end_time = log_reader.end_time
            # Assert to prevent "Incompatible types in assignment" with Optional
            if end_time is None:
                raise RuntimeError(
                    "log_reader.end_time is None although " "log_reader.ended() is True"
                )
            metadata_updater.update_stop_time(end_time)
            latest_status = "complete"
        else:
            latest_status = self.check_if_running_or_errored(log_reader)
        return latest_status

    @staticmethod
    def check_if_running_or_errored(log_reader: LogReader) -> str:
        """
        Check if a run is still running or has errored.

        Parameters
        ----------
        log_reader : LogReader
            The object which reads log files

        Returns
        -------
        latest_status : str
            The latest status
        """
        pid = log_reader.pid
        if pid is None:
            latest_status = "created"
        elif psutil.pid_exists(pid):
            latest_status = "running"
        else:
            latest_status = "error"
        return latest_status
