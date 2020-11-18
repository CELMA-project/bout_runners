"""Contains the abstract cluster submitter class."""
import logging
import re
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.file_operations import get_caller_dir


class AbstractClusterSubmitter(AbstractSubmitter):
    """
    The abstract cluster class of the submitters.

    Attributes
    ----------
    _cluster_specific : dict
        Dict containing the commands for cancelling a job, releasing a job and
        submit a job for the inherited object
    _job_name : str
        Getter and setter variable for job_name
    _log_and_error_base : Path
        Base for the path for the .log and .err files
    _store_dir : Path
        Getter and setter variable for store_dir
    _submission_dict : dict
        Dict containing walltime, mail, queue and account info
    _released : bool
        Getter variable for waiting_for
    _waiting_for : tuple of str
        Getter variable for released
    job_name : str
        Name of the job
    released : bool
        Whether or not the job has been released to the queue
    store_dir : Path
        Directory to store the script
    waiting_for : tuple of str
        Tuple of job names which this job is waiting for

    Methods
    -------
    _populate_std_out_and_std_err()
        Populate std_out and std_err
    get_return_code(sacct_str)
        Return the exit code if any
    create_submission_string(command, waiting_for)
        Create the submission string
    get_days_hours_minutes_seconds_from_str(time_str)
        Return days, hours, minutes, seconds from the string
    add_waiting_for(waiting_for_id)
        Add a waiting for id to the waiting for list
    kill()
        Kill a job if it exists
    release()
        Release job if held
    submit_command(command)
        Submit a command
    raise_error()
        Raise and error from the subprocess in a clean way
    """

    def __init__(
        self,
        job_name: Optional[str] = None,
        store_dir: Optional[Path] = None,
        submission_dict: Optional[Dict[str, Optional[str]]] = None,
        processor_split: Optional[ProcessorSplit] = None,
    ) -> None:
        """
        Set the member data.

        Parameters
        ----------
        job_name : str or None
            Name of the job
            If None, a timestamp will be given as job_name
        store_dir : Path or None
            Directory to store the script
            If None, the caller directory will be used as the store directory
        submission_dict : None or dict of str of None or str
            Dict containing optional submission options
            One the form

            >>> {'walltime': None or str,
            ...  'mail': None or str,
            ...  'queue': None or str,
            ...  'account': None or str}

            These options will not be used if the submission_dict is None
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        super().__init__(processor_split)
        if job_name is None:
            self._job_name = datetime.now().strftime("%m-%d-%Y_%H-%M-%S-%f")
        else:
            self._job_name = job_name

        if store_dir is None:
            self._store_dir = get_caller_dir()
        else:
            self._store_dir = store_dir
        self._submission_dict = (
            submission_dict.copy() if submission_dict is not None else dict()
        )
        submission_dict_keys = self._submission_dict.keys()
        for key in ("walltime", "mail", "queue", "account"):
            if key not in submission_dict_keys:
                self._submission_dict[key] = None

        self._log_and_error_base: Path = Path()
        self._waiting_for: List[str] = list()
        self._released = False

        # The following will be set by the implementations
        self._cluster_specific = {"cancel_str": "", "release_str": "", "submit_str": ""}

    def _populate_std_out_and_std_err(self) -> None:
        """Populate std_out and std_err."""
        if self._status["return_code"] is not None:
            log_path = self._log_and_error_base.parent.joinpath(
                f"{self._log_and_error_base.stem}.log"
            )
            if log_path.is_file():
                with log_path.open("r") as file:
                    self._status["std_out"] = file.read()
            else:
                self._status["std_out"] = ""
                logging.warning(
                    "No log file found in %s for %s when populating std_out. "
                    "Did the submission quit unexpectedly?",
                    log_path,
                    self.job_name,
                )

            err_path = self._log_and_error_base.parent.joinpath(
                f"{self._log_and_error_base.stem}.err"
            )
            if err_path.is_file():
                with err_path.open("r") as file:
                    self._status["std_err"] = file.read()
            else:
                self._status["std_err"] = ""
                logging.warning(
                    "No err file found in %s for %s when populating std_err. "
                    "Did the submission quit unexpectedly?",
                    err_path,
                    self.job_name,
                )

            logging.debug(
                "std_out and std_err populated for job_id %s (%s)",
                self._status["job_id"],
                self.job_name,
            )

    @staticmethod
    @abstractmethod
    def extract_job_id(std_out: Optional[str]) -> str:
        """
        Return the job_id.

        Parameters
        ----------
        std_out : str or None
            The standard output from the local submitter which submits the job

        Returns
        -------
        job_id : str
            The job id
        """

    @abstractmethod
    def create_submission_string(
        self, command: str, waiting_for: Tuple[str, ...]
    ) -> str:
        """
        Create the submission string.

        Parameters
        ----------
        command : str
            The command to submit
        waiting_for : tuple of str
            Tuple of ids that this job will wait for
        """

    @staticmethod
    def get_days_hours_minutes_seconds_from_str(
        time_str: str,
    ) -> Tuple[int, int, int, int]:
        """
        Return days, hours, minutes, seconds from the string.

        Parameters
        ----------
        time_str : str
            Must be on the format

            >>> 'hh:mm:ss'

            or

            >>> 'd-hh:mm:ss'

        Returns
        -------
        days : int
            Number of days in the time string
        hours : int
            Number of hours in the time string
        minutes : int
            Number of minutes in the time string
        seconds : int
            Number of seconds in the time string

        Raises
        ------
        ValueError
            If the string is malformatted
        """
        slurm_pattern = r"(\d)-(\d{2}):(\d{2}):(\d{2})"
        pbs_pattern = r"(\d{2}):(\d{2}):(\d{2})"
        slurm_search = re.search(slurm_pattern, time_str)
        pbs_search = re.search(pbs_pattern, time_str)
        if slurm_search is not None:
            days = int(slurm_search.group(1))
            hours = int(slurm_search.group(2))
            minutes = int(slurm_search.group(3))
            seconds = int(slurm_search.group(4))
        elif pbs_search is not None:
            days = 0
            hours = int(pbs_search.group(1))
            minutes = int(pbs_search.group(2))
            seconds = int(pbs_search.group(3))
        else:
            msg = f"Could not extract time from {time_str}"
            logging.error(msg)
            raise ValueError(msg)
        return days, hours, minutes, seconds

    @property
    def job_name(self) -> str:
        """
        Set the properties of self.job_name.

        Returns
        -------
        str
            The job name
        """
        return self._job_name

    @job_name.setter
    def job_name(self, job_name: str) -> None:
        old_name = self._job_name
        self._job_name = job_name
        logging.debug("job_name changed from %s to %s", old_name, self._job_name)

    @property
    def released(self) -> bool:
        """
        Return whether the job has been released to the cluster.

        Returns
        -------
        bool
            True if the job is not held in the cluster
        """
        return self._released

    @property
    def store_dir(self) -> Path:
        """
        Set the properties of self.store_dir.

        Returns
        -------
        Path
            Path to the store directory
        """
        return self._store_dir

    @store_dir.setter
    def store_dir(self, store_dir: Union[str, Path]) -> None:
        self._store_dir = Path(store_dir).absolute()
        logging.debug("store_dir changed to %s", store_dir)

    @property
    def waiting_for(self) -> Tuple[str, ...]:
        """
        Return the waiting for list as a tuple.

        Returns
        -------
        tuple of str
            The waiting for list as a tuple
        """
        return tuple(self._waiting_for)

    def add_waiting_for(
        self, waiting_for_id: Union[Optional[str], Iterable[str]]
    ) -> None:
        """
        Add a waiting for id to the waiting for list.

        This will waiting for list will be written to the submission string
        upon creation

        Parameters
        ----------
        waiting_for_id : None or list of str
            Id to the job waiting for
        """
        if waiting_for_id is not None:
            if isinstance(waiting_for_id, str):
                self._waiting_for.append(waiting_for_id)
                logging.debug(
                    "Adding the following to the waiting_for_list for %s: %s",
                    self.job_name,
                    waiting_for_id,
                )
            else:
                for waiting_id in waiting_for_id:
                    self._waiting_for.append(waiting_id)
                    logging.debug(
                        "Adding the following to the waiting_for_list for %s: %s",
                        self.job_name,
                        waiting_id,
                    )

    def kill(self) -> None:
        """Kill a job if it exists."""
        if self.job_id is not None and not self.completed():
            logging.info("Killing job_id %s (%s)", self.job_id, self.job_name)
            submitter = LocalSubmitter()
            submitter.submit_command(
                f"{self._cluster_specific['cancel_str']} {self.job_id}"
            )
            submitter.wait_until_completed()
            self._released = True

    def release(self) -> None:
        """Release job if held."""
        if self.job_id is not None and not self._released:
            logging.debug("Releasing job_id %s (%s)", self.job_id, self.job_name)
            submitter = LocalSubmitter()
            submitter.submit_command(
                f"{self._cluster_specific['release_str']} {self.job_id}"
            )
            submitter.wait_until_completed()
            self._released = True

    def submit_command(self, command: str) -> None:
        """
        Submit a command.

        Notes
        -----
        All submitted jobs are held
        Release with self.release
        See [1]_ for details

        Parameters
        ----------
        command : str
            Command to submit

        References
        ----------
        .. [1] https://community.openpbs.org/t/ignoring-finished-dependencies/1976
        """
        # This starts the job anew, so we restart the instance to clear it from any
        # spurious member data, before doing so, we must capture the waiting for tuple
        waiting_for = self.waiting_for
        self.reset()
        script_path = self.store_dir.joinpath(f"{self._job_name}.sh")
        with script_path.open("w") as file:
            file.write(self.create_submission_string(command, waiting_for=waiting_for))

        # Make the script executable
        local_submitter = LocalSubmitter(run_path=self.store_dir)
        local_submitter.submit_command(f"chmod +x {script_path}")
        local_submitter.wait_until_completed()

        # Submit the command through a local submitter
        local_submitter.submit_command(
            f"{self._cluster_specific['submit_str']} {script_path}"
        )
        local_submitter.wait_until_completed()
        self._status["job_id"] = self.extract_job_id(local_submitter.std_out)
        logging.info(
            "job_id %s (%s) given to command '%s' in %s",
            self.job_id,
            self.job_name,
            command,
            script_path,
        )

    def raise_error(self) -> None:
        """
        Raise and error from the subprocess in a clean way.

        Raises
        ------
        RuntimeError
            If an error was caught
        """
        if self.completed():
            if self.return_code != 0:
                if self.return_code is None:
                    msg = (
                        "Submission was never submitted. "
                        "Did some of the dependencies finished before "
                        "submitting the job? "
                        "In that case the finished dependency might have "
                        "rejected the job."
                    )
                    logging.critical(msg)
                    raise RuntimeError(msg)
                msg = f"Submission errored with error code {self.return_code}"
                logging.critical(msg)
                raise RuntimeError(msg)
