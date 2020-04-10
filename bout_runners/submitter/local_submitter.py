"""Contains the local submitter class."""


import logging
import subprocess
from pathlib import Path
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit


class LocalSubmitter(AbstractSubmitter):
    """
    Submits a command.

    Attributes
    ----------
    __pid : None or int
        Getter variable for pid
    path : Path or str
        Directory to run the command from
    processor_split : ProcessorSplit
        Object containing the processor split
    pid : None or int
        The processor id

    Methods
    -------
    submit_command(command)
        Run a subprocess
    _raise_submit_error(self, result):
        Raise and error from the subprocess in a clean way.
    """

    def __init__(self, path='', processor_split=ProcessorSplit()):
        """
        Set the path from where the calls are made from.

        Parameters
        ----------
        path : Path or str
            Directory to run the command from
        processor_split : ProcessorSplit
            Object containing the processor split
        """
        self.__path = Path(path).absolute()
        self.processor_split = processor_split
        self.__pid = None

    @property
    def pid(self):
        """Return the process id."""
        return self.__pid

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
        # This is a simplified subprocess.run(), with the exception
        # that we capture the process id
        process = subprocess.Popen(command.split(),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=self.__path)
        std_out, std_err = process.communicate()
        return_code = process.poll()
        result = \
            subprocess.CompletedProcess(process.args,
                                        return_code,
                                        std_out,
                                        std_err)
        self.__pid = process.pid

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
