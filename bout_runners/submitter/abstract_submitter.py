"""Contains the abstract submitter class."""


from abc import ABC
from abc import abstractmethod


class AbstractSubmitter(ABC):
    """The abstract base class of the submitters."""

    @abstractmethod
    def submit_command(self, command):
        """Submit a command."""
        pass

    @abstractmethod
    def _raise_submit_error(self, error):
        """Raise error if submission failed."""
        pass
