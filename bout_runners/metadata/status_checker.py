"""Module containing the StatusChecker class."""


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

    def check_status(self):
        """FIXME"""
        # Check that run table exist
        # Use DatabaseReader to check for status submitted or
        #    possible crash in run table
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
