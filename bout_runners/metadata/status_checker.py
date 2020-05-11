"""Module containing the StatusChecker class."""


import logging
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.log.log_reader import LogReader
from bout_runners.metadata.metadata_updater import MetadataUpdater


class StatusChecker:
    """
    FIXME
    """
    # Whenever we are reading from database we would like to update,
    # however not so nice to mix reader and writer through the status
    # checker
    # Is this rather status updater?
    # An alternative could be to put it in metadatawriter reader and
    # writer
    # Can also be called independently

    def __init__(self, database_connector, bout_paths):
        """
        FIXME

        Notes
        -----
        The StatusChecker instance only checks the project belonging
        to the same database schema grouped together by the
        database_connector

        Parameters
        ----------
        database_connector
        bout_paths
        """
        self.__database_connector = database_connector
        self.__database_reader = \
            DatabaseReader(self.__database_connector)
        self.__bout_paths = bout_paths

    def check_status(self):
        """FIXME"""
        # Check that run table exist
        if not self.__database_reader.check_tables_created():
            logging.warning(
                'No tables found in %s',
                self.__database_reader.database_connector.database_path)
            # FIXME: What should the exit be here?

        query = (
            "SELECT name, id AS run_id FROM run WHERE\n"
            "latest_status = 'submitted'")
        submitted_to_check = self.__database_reader.query(query)

        latest_status = 'submitted'

        metadata_updater = \
            MetadataUpdater(self.__database_connector, run_id=-1)

        for name, run_id in submitted_to_check.itertuples(index=False):
            metadata_updater.run_id = run_id

            log_path = self.__bout_paths.joinpath(name, 'BOUT.log.0')
            log_reader = LogReader(log_path)
            if log_path.is_file():
                if log_reader.started():
                    start_time = log_reader.start_time
                    metadata_updater.update_start_time(start_time)
                    if log_reader.ended():
                        end_time = log_reader.end_time
                        metadata_updater.update_stop_time(end_time)
                        latest_status = 'complete'
                    else:
                        # Check if the process is still running
                        # FIXME: You are here
                        pid = log_reader.pid
                        # FIXME: Consider using pid, else
                        #  https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
                        if exist_pid(pid):
                            latest_status = 'running'
                        else:
                            # FIXME: Check that the process didn't
                            #  errored or finished in the meantime
                            latest_status = 'error'
                else:
                    # No started time is found in the log
                    if log_reader.pid_exist():
                        # FIXME: This is the same as above
                        pid = log_reader.pid
                        if exist_pid(pid):
                            latest_status = 'running'
                        else:
                            # FIXME: Check that the process didn't
                            #  errored or finished in the meantime
                            latest_status = 'error'
                    else:
                        # FIXME: Wait and check again, if fails ->
                        #  possible crash. If possible_crash,
                        #  this should also be queried for
                        pass
            else:
                # No log file exists
                # FIXME: Either queued or failed...how to find out if
                #  queued? If queued -> need to check for these
                pass
        # FIXME: You are here

        metadata_updater.update_latest_status(latest_status)

        query = (
            "SELECT name FROM run WHERE\n"
            "latest_status = 'running'")
        running_to_check = self.__database_reader.query(query)
        # Use DatabaseReader to check for status submitted or
        #    possible_crash in run table
        # From there get dir to check in
        # Check for BOUT.log.0
        # If BOUT.log.0 exist -> can search for pid
        #     Can check if pid start time is somewhat equal to
        #     submitted time?
        #     Possibility to use psutil
        # If not found, status is submitted
        #    Can also be in queue?
        # If file found and not found time started -> crashed before
        #    status possible crashed
        # If found, check for started time in log
        #
        pass

        # Data files: BOUT.log.0
        # If a job has been canceled, the log is just short
        #
