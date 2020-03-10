"""Contains the local submitter class."""


import logging
import subprocess
from pathlib import Path
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit

# FIXME: Make this a factory pattern. Local, PBS or SLURM can be
#  selected. Hard to figure out how to test though...
# FIXME: Should this be a separate repo? Seems like it is multi
#  purpose, would possibly also be easier with testing etc.
class LocalSubmitter(AbstractSubmitter):
    """
    Submits a command.

    Attributes
    ----------
    path : Path or str
        Directory to run the command from
    processor_split : ProcessorSplit
        Object containing the processor split

    Methods
    -------
    submit_command(command)
        Run a subprocess
    _raise_submit_error(self, result):
        Raise and error from the subprocess in a clean way.
    """

    def __init__(self, path, processor_split=ProcessorSplit()):
        """
        Set the path from where the calls are made from

        Parameters
        ----------
        path : Path or str
            Directory to run the command from
        processor_split : ProcessorSplit
            Object containing the processor split
        """
        self.__path = Path(path)
        self.processor_split = processor_split

    def submit_command(self, command):
        """
        Run a subprocess.

        Parameters
        ----------
        command : str
            The command to run

        Returns
        -------
        result : subprocess.CompletedProcess
            The result of the subprocess call
        """
        logging.info('Executing %s in %s', command, self.__path)
        result = subprocess.run(command.split(),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                cwd=self.__path,
                                check=False)

        if result.returncode != 0:
            self._raise_submit_error(result)

        return result

    def _raise_submit_error(self, result):
        """
        Raise and error from the subprocess in a clean way.

        Parameters
        ----------
        result : subprocess.CompletedProcess
            The result from the subprocess
        """
        logging.error('Subprocess failed with stdout:')
        logging.error(result.stdout)
        logging.error('and stderr:')
        logging.error(result.stderr)

        result.check_returncode()
