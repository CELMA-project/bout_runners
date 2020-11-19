"""Contains the local submitter class."""


import logging

# NOTE: Subprocess below is safe against shell injections
# https://github.com/PyCQA/bandit/issues/280
import subprocess  # nosec
from pathlib import Path
from typing import Optional

from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.file_operations import get_caller_dir


class LocalSubmitter(AbstractSubmitter):
    """
    Submits a command.

    Attributes
    ----------
    __process : None or Popen
        The Popen process if it has been created
    run_path : Path or str
        Directory to run the command from

    Methods
    -------
    _wait_for_std_out_and_std_err()
        Wait until the process completes, populate return_code, std_out and std_err
    submit_command(command)
        Submit a subprocess
    completed()
        Return the completed status
    raise_error(self)
        Raise and error from the subprocess in a clean way

    Examples
    --------
    >>> submitter = LocalSubmitter()
    >>> submitter.submit_command('ls')
    >>> submitter.wait_until_completed()
    >>> print(submitter.std_out)
    __init__.py
    test_local_submitter.py
    test_processor_split.py
    test_submitter_factory.py
    """

    def __init__(
        self,
        run_path: Optional[Path] = None,
        processor_split: Optional[ProcessorSplit] = None,
    ) -> None:
        """
        Set the path from where the calls are made from.

        Parameters
        ----------
        run_path : Path or str or None
            Directory to run the command from
            If None, the calling directory will be used
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        AbstractSubmitter.__init__(self, processor_split)
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.run_path = (
            Path(run_path).absolute() if run_path is not None else get_caller_dir()
        )
        self.__process: Optional[subprocess.Popen] = None

    def _wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes if a process has been started.

        Populate return_code, std_out and std_err
        """
        if self.__process is not None:
            std_out, std_err = self.__process.communicate()
            self._status["return_code"] = self.__process.poll()
            self._status["std_out"] = std_out.decode("utf8").strip()
            self._status["std_err"] = std_err.decode("utf8").strip()
        else:
            logging.warning(
                "No process started, return_code, std_out, std_err not populated"
            )

    def submit_command(self, command: str) -> None:
        """
        Submit a subprocess.

        Parameters
        ----------
        command : str
            The command to run
        """
        # This starts the job anew, so we restart the instance to clear it from any
        # spurious member data
        self.reset()
        self.__process = subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.run_path,
            # https://docs.python.org/3/library/subprocess.html#security-considerations
            # https://github.com/PyCQA/bandit/issues/280
            shell=False,  # nosec
        )
        self._status["job_id"] = str(self.__process.pid)
        logging.debug(
            "job_id %s given to command '%s' in %s", self.job_id, command, self.run_path
        )

    def completed(self) -> bool:
        """
        Return the completed status.

        Communicate the process has completed.

        Returns
        -------
        bool
            True if the process has completed
        """
        if self.__process is not None:
            if self.return_code is not None:
                return True
            return_code = self.__process.poll()
            if return_code is not None:
                self._status["return_code"] = return_code
                self._wait_for_std_out_and_std_err()
                return True
        return False

    def raise_error(self) -> None:
        """Raise and error from the subprocess in a clean way."""
        if self.completed():
            if isinstance(self.__process, subprocess.Popen) and isinstance(
                self.return_code, int
            ):
                result = subprocess.CompletedProcess(
                    self.__process.args, self.return_code, self.std_out, self.std_err
                )

                result.check_returncode()
