"""Contains the local submitter class."""


import logging
import subprocess
from pathlib import PosixPath, Path
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.file_operations import get_caller_dir
from subprocess import CompletedProcess
from typing import Optional


class LocalSubmitter(AbstractSubmitter):
    r"""
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

    Examples
    --------
    >>> LocalSubmitter().submit_command('ls')
    CompletedProcess(args=['ls'], returncode=0, stdout=b'__init__.py\n
    __pycache__\n
    test_local_submitter.py\n
    test_processor_split.py\n
    test_submitter_factory.py\n', stderr=b'')
    """

    def __init__(
        self, path: Optional[PosixPath] = None, processor_split: None = None
    ) -> None:
        """
        Set the path from where the calls are made from.

        Parameters
        ----------
        path : Path or str or None
            Directory to run the command from
            If None, the calling directory will be used
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.__path = Path(path).absolute() if path is not None else get_caller_dir()
        self.processor_split = (
            processor_split if processor_split is not None else ProcessorSplit()
        )
        self.__pid = None

    @property
    def pid(self) -> int:
        """Return the process id."""
        return self.__pid

    def submit_command(self, command: str) -> CompletedProcess:
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
        logging.info("Executing %s in %s", command, self.__path)
        # This is a simplified subprocess.run(), with the exception
        # that we capture the process id
        process = subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.__path,
        )
        std_out, std_err = process.communicate()
        return_code = process.poll()
        result = subprocess.CompletedProcess(
            process.args, return_code, std_out, std_err
        )
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
        logging.error("Subprocess failed with stdout:")
        logging.error(result.stdout)
        logging.error("and stderr:")
        logging.error(result.stderr)

        result.check_returncode()
