"""Contains the abstract submitter class."""


import json
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.serializers import is_jsonable


class AbstractSubmitter(ABC):
    """
    The abstract base class of the submitters.

    Attributes
    ----------
    _logged_complete_status : bool
        Whether the complete status has been logged
    _status : dict of str
        Status of the submission
    processor_split : ProcessorSplit
        Object containing the processor split
    job_id : None or str
        The processor id if the process has started
    return_code : None or int
        The return code if the process has finished
    std_out : None or str
        The standard output if the process has finished
    std_err : None or str
        The standard error if the process has finished

    Methods
    -------
    _reset_status()
        Reset the status dict
    _catch_error
        Log the error
    _wait_for_std_out_and_std_err
       Wait until the process completes if a process has been started
    submit_command(command)
        Submit a command
    completed()
        Return the completed status
    raise_error()
        Raise and error from the subprocess in a clean way
    write_python_script(path, function, args, kwargs)
        Write python function to file
    reset()
        Reset the submitter
    wait_until_completed(raise_error)
        Wait until the process has completed
    errored(raise_error)
        Return True if the process errored
    """

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
        self._reset_status()

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
            logging.info("Start: Waiting for jobs to complete")
            self._wait_for_std_out_and_std_err()
            self.errored(raise_error)
            logging.info("Done: Waiting for jobs to complete")

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
                logging.debug("job_id %s completed successfully", self.job_id)
                self._logged_complete_status = True
        return False
