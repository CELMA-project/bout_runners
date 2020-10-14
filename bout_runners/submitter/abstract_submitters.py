"""Contains the abstract submitter classes."""

import sys
import re
import logging
import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Tuple, Dict
from pathlib import Path

from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.serializers import is_jsonable


class AbstractSubmitter(ABC):
    """The abstract base class of the submitters."""

    @abstractmethod
    def submit_command(self, command: str) -> Any:
        """
        Submit a command.

        Parameters
        ----------
        command : str
            Command to submit
        """

    @property
    @abstractmethod
    def pid(self) -> Optional[int]:
        """Return the process id."""

    @property
    @abstractmethod
    def return_code(self) -> Optional[int]:
        """Return the return code."""

    @property
    @abstractmethod
    def std_out(self) -> Optional[str]:
        """Return the standard output."""

    @property
    @abstractmethod
    def std_err(self) -> Optional[str]:
        """Return the standard error."""

    @staticmethod
    def write_python_script(
        path: Path,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write python function to file.

        Parameters
        ----------
        path : Path
            Absolute path to store the python file which holds the function and
            its arguments
        function : function
            The function to call
        args : tuple
            The positional arguments
        kwargs : dict
            The keyword arguments
        """
        # Make a string of the arguments
        if args is not None:
            args_list = list(args)
            for index, arg in enumerate(args_list):
                if not is_jsonable(arg):
                    logging.warning(
                        "The argument %s is not jsonable. "
                        "Will try to cast it to a string",
                        arg,
                    )
                    args_list[index] = str(arg)
            # Use starred expressions due to json dumps
            args_str = f"*{json.dumps(args_list)}"
        else:
            args_str = ""

        # Make a string of the keyword arguments
        if kwargs is not None:
            if args is not None:
                args_str += ", "
            keyword_arguments = dict()
            for arg_name, value in kwargs.items():
                if is_jsonable(value):
                    keyword_arguments[arg_name] = value
                else:
                    logging.warning(
                        "The value %s of %s is not jsonable. "
                        "Will try to cast it to a string",
                        value,
                        arg_name,
                    )
                    keyword_arguments[arg_name] = str(value)
            # Use starred expressions due to json dumps
            kwargs_str = f"**{json.dumps(keyword_arguments)}"
        else:
            kwargs_str = ""

        # Make the script
        script_str = (
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            f"sys.path = {sys.path}\n"
            f"from {function.__module__} import {function.__name__}\n"
            f"{function.__name__}({args_str}{kwargs_str})"
        )

        # Write the python script
        with path.open("w") as python_file:
            python_file.write(script_str)
        logging.info("Python script written to %s", path)

    @abstractmethod
    def wait_until_completed(self, raise_error: bool = True) -> None:
        """
        Wait until the process has completed.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors
        """

    @abstractmethod
    def completed(self) -> bool:
        """Return the completed status."""

    @abstractmethod
    def errored(self, raise_error: bool = False) -> bool:
        """
        Return True if the process errored.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors
        """

    @abstractmethod
    def raise_error(self) -> None:
        """Raise and error from the subprocess in a clean way."""


class AbstractClusterSubmitter(ABC):
    """The abstract cluster class of the submitters."""

    def __init__(
        self,
        job_name: str,
        store_path: Path,
        submission_dict: Optional[Dict[str, Optional[str]]] = None,
        processor_split: Optional[ProcessorSplit] = None,
    ) -> None:
        """
        Set the member data.

        Parameters
        ----------
        job_name : str
            Name of the job
        store_path : path
            Path to store the script
        submission_dict : None or dict of str of None or str
            Dict containing optional submission options
            One the form

            >>> {'walltime': None or str,
            ...  'mail': None or str,
            ...  'queue': None or str,
            ...  'account': None or str}

            These options will not be used if the submission_dict is None
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        self.__job_name = job_name
        self.__store_path = store_path
        self.__processor_split = (
            processor_split if processor_split is not None else ProcessorSplit()
        )
        self.__submission_dict = (
            submission_dict.copy() if submission_dict is not None else dict()
        )
        submission_dict_keys = self.__submission_dict.keys()
        for key in ("walltime", "mail", "queue", "account"):
            if key not in submission_dict_keys:
                self.__submission_dict[key] = None

    @abstractmethod
    def create_submission_string(self, command: str) -> str:
        """
        Create the submission string.

        Parameters
        ----------
        command : str
            The command to submit
        """

    @staticmethod
    def get_days_hours_minutes_seconds_from_str(
        time_str: str,
    ) -> Tuple[int, int, int, int]:
        """
        Return days, hours, minutes, seconds from the string.

        Parameters
        ----------
        time_str : str
            Must be on the format

            >>> 'hh:mm:ss'

            or

            >>> 'd-hh:mm:ss'

        Returns
        -------
        days : int
            Number of days in the time string
        hours : int
            Number of hours in the time string
        minutes : int
            Number of minutes in the time string
        seconds : int
            Number of seconds in the time string

        Raises
        ------
        ValueError
            If the string is malformatted
        """
        slurm_pattern = r"(\d)-(\d{2}):(\d{2}):(\d{2})"
        pbs_pattern = r"(\d{2}):(\d{2}):(\d{2})"
        slurm_search = re.search(slurm_pattern, time_str)
        pbs_search = re.search(pbs_pattern, time_str)
        if slurm_search is not None:
            days = int(slurm_search.group(1))
            hours = int(slurm_search.group(2))
            minutes = int(slurm_search.group(3))
            seconds = int(slurm_search.group(4))
        elif pbs_search is not None:
            days = 0
            hours = int(pbs_search.group(1))
            minutes = int(pbs_search.group(2))
            seconds = int(pbs_search.group(3))
        else:
            msg = f"Could not extract time from {time_str}"
            logging.error(msg)
            raise ValueError(msg)
        return days, hours, minutes, seconds
