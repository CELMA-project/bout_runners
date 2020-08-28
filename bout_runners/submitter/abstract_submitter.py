"""Contains the abstract submitter class."""

import sys
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Tuple, Dict
from pathlib import Path


class AbstractSubmitter(ABC):
    """The abstract base class of the submitters."""

    @abstractmethod
    def submit_command(self, command: str):
        """
        Submit a command.

        Parameters
        ----------
        command : str
            Command to submit
        """

    @property
    @abstractmethod
    def pid(self):
        """Return the process id."""

    @staticmethod
    def write_python_script(
        path: Path,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
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
            args_str = ", ".join(map(str, args))
        else:
            args_str = ""

        # Make a string of the keyword arguments
        if kwargs is not None:
            if args is not None:
                args_str += ", "
            keyword_arguments = []
            for arg_name, value in kwargs.items():
                if isinstance(value, str):
                    value = f"'{value}'"
                keyword_arguments.append(f"{arg_name}={value}")

            # Put a comma in between the arguments
            kwargs_str = ", ".join(map(str, keyword_arguments))
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
    def _raise_submit_error(self, result: Any):
        """
        Raise error if submission failed.

        Parameters
        ----------
        result : object
            The result from the subprocess
        """
