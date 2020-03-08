"""Contains the local submitter class."""


import logging
import subprocess
from bout_runners.submitter.abstract_submitter import AbstractSubmitter


# FIXME: Make this a factory pattern. Local, PBS or SLURM can be
#  selected. Hard to figure out how to test though...
# FIXME: Copy the run_subprocess from subprocess_functions here
class LocalSubmitter(AbstractSubmitter):
    """
    Submits a command.

    FIXME: Add variables and attributes

    FIXME: Add examples
    """

    def __init__(self, path):
        """
        FIXME

        Parameters
        ----------
        path : Path or str
            Path to the location to run the command from
        """
        self.__path = path

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
