"""Contains the abstract submitter class."""


from abc import ABC
from abc import abstractmethod


class AbstractSubmitter(ABC):

    # FIXME: Complete with input
    @abstractmethod
    def __init__(self, path):
        pass

    @abstractmethod
    def submit_command(self, command):
        pass

    @abstractmethod
    def _raise_submit_error(self, error):
        pass
