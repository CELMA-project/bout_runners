"""Module containing the LogReader class."""


import re
import logging
from datetime import datetime
from pathlib import Path


class LogReader:
    """
    FIXME
    """

    def __init__(self, log_path):
        """
        FIXME
        """
        with Path(log_path).open('r') as log_file:
            self.file_str = log_file.read()
            logging.debug('Opened logfile %s', log_path)

    def started(self):
        """
        Check whether the run has a start time.

        Returns
        -------
        bool
            True if the start signature is found in the file
        """
        return self.__is_str_in_file(r'^Run started at')

    def ended(self):
        """
        Check whether the run has an end time.

        Returns
        -------
        bool
            True if the end signature is found in the file
        """
        return self.__is_str_in_file(r'^Run finished at')

    @property
    def start_time(self):
        """FIXME"""
        if self.started:
            self.__find_locale_time(r'^Run started at  : (.*)')
        else:
            return None

    @property
    def end_time(self):
        """FIXME"""
        if self.started:
            self.__find_locale_time(r'^Run finished at  : (.*)')
        else:
            return None

    def __is_str_in_file(self, pattern):
        match = re.match(pattern, self.file_str)
        if match is None:
            return False
        else:
            return True

    def __find_locale_time(self, pattern):
        start_time_str = re.match(pattern, self.file_str).group(1)
        start_time = datetime.strptime(start_time_str, '%c')
        return start_time
