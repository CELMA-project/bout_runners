"""Contains the abstract submitter class."""

import sys
import logging
import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Tuple, Dict
from pathlib import Path
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
    def _raise_submit_error(self, result: Any) -> None:
        """
        Raise error if submission failed.

        Parameters
        ----------
        result : object
            The result from the subprocess
        """
