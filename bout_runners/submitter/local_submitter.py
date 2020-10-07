"""Contains the local submitter class."""


import logging

# NOTE: Subprocess below is safe against shell injections
# https://github.com/PyCQA/bandit/issues/280
import subprocess  # nosec
from pathlib import Path

from typing import Optional, Union, Dict

from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.file_operations import get_caller_dir


class LocalSubmitter(AbstractSubmitter):
    r"""
    Submits a command.

    Attributes
    ----------
    __status : dict of str
        Status of the submission
    __path : Path or str
        Directory to run the command from
    __process : None or Popen
        The Popen process if it has been created
    __logged_complete_status : bool
        Whether the complete status has been logged
    processor_split : ProcessorSplit
        Object containing the processor split
    pid : None or int
        The processor id if the process has started
    return_code : None or int
        The return code if the process has finished
    std_out : None or str
        The standard output if the process has finished
    std_err : None or str
        The standard error if the process has finished

    Methods
    -------
    __wait_for_std_out_and_std_err()
        Wait until the process completes, populate return_code, std_out and std_err
    __catch_error()
        Log the error
    submit_command(command)
        Run a subprocess
    write_python_script(path, function, args, kwargs)
        Write python function to file
    completed()
        Return the completed status
    errored(raise_error)
        Return True if the process errored
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
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.__path = (
            Path(run_path).absolute() if run_path is not None else get_caller_dir()
        )
        self.__process: Optional[subprocess.Popen] = None

        self.__logged_complete_status = False

        # Attributes with getters
        self.__status: Dict[str, Union[Optional[int], Optional[str]]] = dict()
        self.__status["pid"] = None
        self.__status["return_code"] = None
        self.__status["std_out"] = None
        self.__status["std_err"] = None

        self.processor_split = (
            processor_split if processor_split is not None else ProcessorSplit()
        )

    @property
    def pid(self) -> Optional[int]:
        """
        Return the process id.

        Returns
        -------
        self.__status["pid"] : int or None
            The process id if a process has been called, else None
        """
        # Added mypy guard as type of key cannot be set separately
        return self.__status["pid"] if isinstance(self.__status["pid"], int) else None

    @property
    def return_code(self) -> Optional[int]:
        """
        Return the return code.

        Returns
        -------
        self.__status["return_code"] : int or None
            The return code if the process has completed
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self.__status["return_code"]
            if isinstance(self.__status["return_code"], int)
            else None
        )

    @property
    def std_out(self) -> Optional[str]:
        """
        Return the standard output.

        Returns
        -------
        self.__status["std_out"] : str or None
            The standard output
            None if the process has not completed
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self.__status["std_out"]
            if isinstance(self.__status["std_out"], str)
            else None
        )

    @property
    def std_err(self) -> Optional[str]:
        """
        Return the standard error.

        Returns
        -------
        self.__status["std_err"] : str or None
            The standard error
            None if the process has not completed
        """
        # Added mypy guard as type of key cannot be set separately
        return (
            self.__status["std_err"]
            if isinstance(self.__status["std_err"], str)
            else None
        )

    def submit_command(self, command: str) -> None:
        """
        Submit a subprocess.

        Parameters
        ----------
        command : str
            The command to run
        """
        self.__process = subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.__path,
            # https://docs.python.org/3/library/subprocess.html#security-considerations
            # https://github.com/PyCQA/bandit/issues/280
            shell=False,  # nosec
        )
        self.__status["pid"] = self.__process.pid
        logging.info("pid %s given %s in %s", self.pid, command, self.__path)

    def wait_until_completed(self, raise_error: bool = True) -> None:
        """
        Wait until the process has completed.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors
        """
        if self.__process is not None:
            self.__wait_for_std_out_and_std_err()
            self.__status["return_code"] = self.__process.poll()
            self.errored(raise_error)

    def __wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes.

        Populate return_code, std_out and std_err
        """
        if self.__process is not None:
            std_out, std_err = self.__process.communicate()
            self.__status["return_code"] = self.__process.poll()
            self.__status["std_out"] = std_out.decode("utf8").strip()
            self.__status["std_err"] = std_err.decode("utf8").strip()

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
            return_code = self.__process.poll()
            if return_code is not None:
                self.__status["return_code"] = return_code
                return True
        return False

    def errored(self, raise_error: bool = False) -> bool:
        """
        Return True if the process errored.

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors

        Returns
        -------
        bool
            True if the process returned a non-zero code
        """
        if self.completed():
            if self.__status["return_code"] != 0:
                self.__catch_error()
                if raise_error:
                    self.raise_error()
                return True
            if not self.__logged_complete_status:
                logging.info("pid %s completed successfully", self.pid)
                self.__logged_complete_status = True
        return False

    def __catch_error(self) -> None:
        """Log the error."""
        if self.completed() and self.return_code != 0:
            self.__wait_for_std_out_and_std_err()

            if not self.__logged_complete_status:
                logging.error(
                    "pid %s failed with return code %s",
                    self.pid,
                    self.return_code,
                )
                logging.error("stdout:")
                logging.error(self.std_out)
                logging.error("stderr:")
                logging.error(self.std_err)
                self.__logged_complete_status = True

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
