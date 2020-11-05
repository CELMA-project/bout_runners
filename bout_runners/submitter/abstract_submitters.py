"""Contains the abstract submitter classes."""

import sys
import re
import logging
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Tuple, Dict, Union
from pathlib import Path

from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.serializers import is_jsonable


class AbstractSubmitter(ABC):
    """The abstract base class of the submitters."""

    def __init__(self, processor_split: Optional[ProcessorSplit] = None) -> None:
        """
        Declare common variables.

        Parameters
        ----------
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        self._logged_complete_status = False
        self._status: Dict[str, Union[Optional[int], Optional[str]]] = dict()
        self.processor_split = (
            processor_split if processor_split is not None else ProcessorSplit()
        )
        self.reset()

    def _reset_status(self) -> None:
        """Reset the status dict."""
        if "job_id" in self._status.keys() and self._status["job_id"] is not None:
            logging.debug(
                "Resetting job_id, return_code, std_out and std_err. "
                "Previous job_id=%s",
                self._status["job_id"],
            )
        self._status["job_id"] = None
        self._status["return_code"] = None
        self._status["std_out"] = None
        self._status["std_err"] = None
        self._logged_complete_status = False

    def _catch_error(self) -> None:
        """Log the error."""
        if self.completed() and self.return_code != 0:

            if not self._logged_complete_status:
                logging.error(
                    "job_id %s failed with return code %s",
                    self.job_id,
                    self.return_code,
                )
                logging.error("stdout:")
                logging.error(self.std_out)
                logging.error("stderr:")
                logging.error(self.std_err)
                self._logged_complete_status = True

    @abstractmethod
    def _wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes if a process has been started.

        Populate return_code, std_out and std_err
        """

    @abstractmethod
    def submit_command(self, command: str) -> Any:
        """
        Submit a command.

        Parameters
        ----------
        command : str
            Command to submit
        """

    @abstractmethod
    def completed(self) -> bool:
        """Return the completed status."""

    @abstractmethod
    def raise_error(self) -> None:
        """Raise and error from the subprocess in a clean way."""

    @property
    def job_id(self) -> Optional[str]:
        """
        Return the process id.

        Returns
        -------
        self._status["job_id"] : int or None
            The process id if a process has been called, else None
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self._status["job_id"] if isinstance(self._status["job_id"], str) else None
        )

    @property
    def return_code(self) -> Optional[int]:
        """Return the return code."""
        # Added mypy guard as type of key cannot be set separately
        return (
            self._status["return_code"]
            if isinstance(self._status["return_code"], int)
            else None
        )

    @property
    def std_out(self) -> Optional[str]:
        """
        Return the standard output.

        Returns
        -------
        self._status["std_out"] : str or None
            The standard output
            None if the process has not completed
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self._status["std_out"]
            if isinstance(self._status["std_out"], str)
            else None
        )

    @property
    def std_err(self) -> Optional[str]:
        """
        Return the standard error.

        Returns
        -------
        self._status["std_err"] : str or None
            The standard error
            None if the process has not completed
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self._status["std_err"]
            if isinstance(self._status["std_err"], str)
            else None
        )

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

    def reset(self) -> None:
        """Reset the submitter."""
        self._reset_status()

    def wait_until_completed(self, raise_error: bool = True) -> None:
        """
        Wait until the process has completed.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors
        """
        if self.job_id is not None and self.return_code is None:
            self._wait_for_std_out_and_std_err()
            self.errored(raise_error)

    def errored(self, raise_error: bool = False) -> bool:
        """
        Return True if the process errored.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors

        Returns
        -------
        bool
            True if the process returned a non-zero code
        """
        if self.completed():
            if self.return_code != 0:
                self._catch_error()
                if raise_error:
                    self.raise_error()
                return True
            if not self._logged_complete_status:
                logging.info("job_id %s completed successfully", self.job_id)
                self._logged_complete_status = True
        return False


class AbstractClusterSubmitter(ABC):
    """The abstract cluster class of the submitters."""

    def __init__(
        self,
        job_name: Optional[str] = None,
        store_dir: Optional[Path] = None,
        submission_dict: Optional[Dict[str, Optional[str]]] = None,
    ) -> None:
        """
        Set the member data.

        Parameters
        ----------
        job_name : str or None
            Name of the job
            If None, a timestamp will be given as job_name
        store_dir : Path or None
            Directory to store the script
            If None, the caller directory will be used as the store directory
        submission_dict : None or dict of str of None or str
            Dict containing optional submission options
            One the form

            >>> {'walltime': None or str,
            ...  'mail': None or str,
            ...  'queue': None or str,
            ...  'account': None or str}

            These options will not be used if the submission_dict is None
        """
        if job_name is None:
            self._job_name = datetime.now().strftime("%m-%d-%Y_%H-%M-%S-%f")
        else:
            self._job_name = job_name

        if store_dir is None:
            self._store_dir = get_caller_dir()
        else:
            self._store_dir = store_dir
        self._submission_dict = (
            submission_dict.copy() if submission_dict is not None else dict()
        )
        submission_dict_keys = self._submission_dict.keys()
        for key in ("walltime", "mail", "queue", "account"):
            if key not in submission_dict_keys:
                self._submission_dict[key] = None

        self._released = False

    @abstractmethod
    def create_submission_string(
        self, command: str, waiting_for: Tuple[str, ...]
    ) -> str:
        """
        Create the submission string.

        Parameters
        ----------
        command : str
            The command to submit
        waiting_for : tuple of str
            Tuple of ids that this job will wait for
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

    @property
    def job_name(self) -> str:
        """
        Set the properties of self.job_name.

        Returns
        -------
        str
            The job name
        """
        return self._job_name

    @job_name.setter
    def job_name(self, job_name: str) -> None:
        old_name = self._job_name
        self._job_name = job_name
        logging.info("job_name changed from %s to %s", old_name, self._job_name)

    @property
    def store_dir(self) -> Path:
        """
        Set the properties of self.store_dir.

        Returns
        -------
        Path
            Path to the store directory
        """
        return self._store_dir

    @store_dir.setter
    def store_dir(self, store_dir: Union[str, Path]) -> None:
        self._store_dir = Path(store_dir).absolute()
        logging.info("store_dir changed to %s", store_dir)
