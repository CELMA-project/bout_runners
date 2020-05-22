"""Contains the abstract submitter class."""


from abc import ABC, abstractmethod
from typing import Any


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

    @abstractmethod
    def _raise_submit_error(self, result: Any):
        """
        Raise error if submission failed.

        Parameters
        ----------
        result : object
            The result from the subprocess
        """
