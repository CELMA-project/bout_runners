"""Contains the abstract submitter class."""


from abc import ABC
from abc import abstractmethod


class AbstractSubmitter(ABC):

    @abstractmethod
    def submit_command(self, command):
        pass

    @abstractmethod
    def _raise_submit_error(self, error):
        pass
