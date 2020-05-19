"""Module containing the LogReader class."""


import re
import logging
from datetime import datetime
from pathlib import Path


class LogReader:
    """
    Class for reading BOUT++ log files.

    Attributes
    ----------
    file_str : str
        The log file as a string
    start_time : None or str
        The time of the execution start (given that it has started)
    end_time : None or str
        The time of the execution end (given that it has started)
    pid : None or int
        The processor id of the part of the program writing to the
        log file

    Methods
    -------
    started()
        Whether or not the execution has started
    ended()
        Whether or not the execution has ended
    pid_exist()
        Whether or not the pid can be found
    __is_str_in_file(pattern)
        Check whether regex-pattern exists in file
    __find_locale_time(pattern)
        Return the locale time of a regex capture

    Examples
    --------
    >>> from pathlib import Path
    >>> path = Path().joinpath('path', 'to', 'BOUT.log.0')
    >>> log_reader = LogReader(path)
    >>> log_reader.start_time
    '2020-05-01 17:07:10'

    >>> log_reader.end_time
    '2020-05-01 17:07:14'

    >>> log_reader.pid
    1190
    """

    def __init__(self, log_path):
        """
        Open and read a log file.

        Parameters
        ----------
        log_path : str or Path
            Absolute path to log file
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

    def pid_exist(self):
        """
        Check whether a process id exist.

        Returns
        -------
        bool
            True if the pid is found
        """
        return self.__is_str_in_file(r'^pid\s*:\s*')

    @property
    def start_time(self):
        """
        Return the start time of the process.

        Returns
        -------
        datetime or None
            The start time on date time format
        """
        if self.started():
            return self.__find_locale_time(r'^Run started at  : (.*)')
        return None

    @property
    def end_time(self):
        """
        Return the end time of the process.

        Returns
        -------
        datetime or None
            The end time on date time format
        """
        if self.ended():
            return self.__find_locale_time(r'^Run finished at  : (.*)')
        return None

    @property
    def pid(self):
        """
        Return the pid of the process.

        Returns
        -------
        int or None
            The pid of the process
        """
        if self.pid_exist():
            pattern = r'^pid:\s*(\d*)\s*$'
            # Using search as match will only search the beginning of
            # the string
            # https://stackoverflow.com/a/32134461/2786884
            return int(re.search(pattern,
                                 self.file_str,
                                 flags=re.MULTILINE).group(1))
        return None

    def __is_str_in_file(self, pattern):
        """
        Check whether regex-pattern exists in file.

        Parameters
        ----------
        pattern : str
            Regex pattern to search for

        Returns
        -------
        bool
            True if pattern exist
        """
        # Using search as match will only search the beginning of the
        # string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, self.file_str, flags=re.MULTILINE)
        if match is None:
            return False
        return True

    def __find_locale_time(self, pattern):
        """
        Return the locale time of a regex capture.

        Parameters
        ----------
        pattern : str
            String to search for

        Returns
        -------
        time : datetime
            The locale datetime
        """
        # Using search as match will only search the beginning of the
        # string
        # https://stackoverflow.com/a/32134461/2786884
        time_str = re.search(pattern,
                             self.file_str,
                             flags=re.MULTILINE).group(1)
        time = datetime.strptime(time_str, '%c')
        return time