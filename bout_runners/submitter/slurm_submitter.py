"""Contains the SLURM submitter class."""


import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Dict, Optional, Tuple

from bout_runners.submitter.abstract_cluster_submitter import AbstractClusterSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit


class SLURMSubmitter(AbstractClusterSubmitter):
    """
    The SLURM submitter class.

    Attributes
    ----------
    __sacct_starttime : str
        Time to search from in sacct

    Methods
    -------
    _wait_for_std_out_and_std_err()
        Wait until the process completes if a process has been started
    extract_job_id(std_out)
        Return the job_id
    get_return_code(sacct_str)
        Return the exit code if any
    get_state(sacct_str)
        Return the state from sacct
    structure_time_to_slurm_format(time_str)
        Structure the time string to a SLURM time string
    completed()
        Return the completed status
    create_submission_string(command, waiting_for)
        Return the PBS script as a string
    get_sacct()
        Return the trace from ``sacct``
    reset()
        Reset released, waiting_for and status dict

    Examples
    --------
    >>> submitter = SLURMSubmitter(job_name, store_path)
    >>> submitter.submit_command("echo 'Hello'")
    >>> submitter.wait_until_completed()
    >>> submitter.std_out
    Hello
    """

    def __init__(
        self,
        job_name: str,
        store_directory: Path,
        submission_dict: Optional[Dict[str, Optional[str]]] = None,
        processor_split: Optional[ProcessorSplit] = None,
    ):
        """
        Set the member data.

        Parameters
        ----------
        job_name : str or None
            Name of the job
            If None, a timestamp will be given as job_name
        store_directory : Path or None
            Directory to store the script
            If None, the caller directory will be used as the store directory
        submission_dict : None or dict of str of None or str
            Dict containing optional submission options
            One the form

            >>> {'walltime': None or str,
            ...  'account': None or str,
            ...  'queue': None or str,
            ...  'mail': None or str}

            These options will not be used if the submission_dict is None
        processor_split : ProcessorSplit or None
            Object containing the processor split
            If None, default values will be used
        """
        super().__init__(job_name, store_directory, submission_dict, processor_split)

        self.__sacct_starttime = (datetime.now() - timedelta(days=365)).strftime(
            r"%Y-%m-%d"
        )

        if self._submission_dict["walltime"] is not None:
            self._submission_dict["walltime"] = self.structure_time_to_slurm_format(
                self._submission_dict["walltime"]
            )

        self._cluster_specific["cancel_str"] = "scancel"
        self._cluster_specific["release_str"] = "scontrol release"
        self._cluster_specific["submit_str"] = "sbatch --hold"

    def _wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes if a process has been started.

        Populate return_code, std_out and std_err
        """
        if self._status["job_id"] is not None:
            self.release()
            while self._status["return_code"] is None:
                sacct_str = self.get_sacct()
                self._status["return_code"] = self.get_return_code(sacct_str)
                sleep(5)
                logging.debug("sacct is reading:\n%s", sacct_str)

            if self._status["return_code"] is not None:
                self._populate_std_out_and_std_err()

        else:
            # No job_id
            logging.warning(
                "Tried to wait for a process without job_id %s (%s). "
                "No process started, so "
                "return_code, std_out, std_err are not populated",
                self.job_id,
                self.job_name,
            )

    @staticmethod
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

        Raises
        ------
        RuntimeError
            If the job_id cannot be found
        """
        if std_out is None:
            msg = "Got std_out=None as input when trying to extract job_id"
            logging.critical(msg)
            raise RuntimeError(msg)
        pattern = "Submitted batch job (.+)"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, std_out)
        if match is None:
            msg = f"Could not extract job_id from the string {std_out}"
            logging.critical(msg)
            raise RuntimeError(msg)
        return match.group(1)

    @staticmethod
    def get_return_code(sacct_str: str) -> Optional[int]:
        """
        Return the exit code if any.

        Parameters
        ----------
        sacct_str : str
            Trace obtained from the ``sacct`` command

        Returns
        -------
        return_code : None or int
            Return code obtained from the cluster

        Notes
        -----
        Assumes the job line is the third line of sacct_str
        (the two others being headers), and that the "ExitCode" is the last
        column in ``sacct``
        """
        job_lines = sacct_str.split("\n")
        if len(job_lines) <= 2:
            return None

        job_line = job_lines[2]
        pattern = r"(-?\d+):-?\d+\s?$"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, job_line, flags=re.MULTILINE)
        if match is None:
            return None
        return int(match.group(1))

    @staticmethod
    def get_state(sacct_str: str) -> Optional[str]:
        """
        Return the state from sacct.

        Parameters
        ----------
        sacct_str : str
            Trace obtained from the ``sacct`` command

        Returns
        -------
        status : None or str
            Status code obtained from the cluster

        Notes
        -----
        Assumes the job line is the third line of sacct_str
        (the two others being headers), and that the "State" is the second last
        column before "ExitCode" in ``sacct``
        """
        job_lines = sacct_str.split("\n")
        if len(job_lines) <= 2:
            return None

        job_line = job_lines[2]
        pattern = r"([A-Z]+\+?)\s+-?\d+:-?\d+\s?$"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, job_line, flags=re.MULTILINE)
        if match is None:
            return None
        return match.group(1)

    @staticmethod
    def structure_time_to_slurm_format(time_str: str) -> str:
        """
        Structure the time string to a SLURM time string.

        Parameters
        ----------
        time_str : str
            Must be on the format
            >>> 'hh:mm:ss'
            or
            >>> 'd-hh:mm:ss'

        Returns
        -------
        str
            The time string formatted as
            >>> 'hh:mm:ss'
        """
        (
            days,
            hours,
            minutes,
            seconds,
        ) = AbstractClusterSubmitter.get_days_hours_minutes_seconds_from_str(time_str)
        days += hours // 24
        hours = hours % 24
        return f"{days}-{hours}:{minutes}:{seconds}"

    def completed(self) -> bool:
        """
        Return the completed status.

        Returns
        -------
        bool
            Whether the job has completed
        """
        if self._status["job_id"] is not None and self._released:
            if self._status["return_code"] is not None:
                return True
            sacct_str = self.get_sacct()
            if self.get_state(sacct_str) in ("RUNNING", None):
                return False
            return_code = self.get_return_code(sacct_str)
            if return_code is not None:
                self._status["return_code"] = return_code
                self._wait_for_std_out_and_std_err()
                return True
        return False

    def create_submission_string(
        self, command: str, waiting_for: Tuple[str, ...]
    ) -> str:
        """
        Return the PBS script as a string.

        Parameters
        ----------
        command : str
            The command to submit
        waiting_for : tuple of str
            Tuple of ids that this job will wait for

        Returns
        -------
        job_script : str
            The script to be submitted
        """
        # Backslash not allowed in f-string expression
        newline = "\n"
        walltime = self._submission_dict["walltime"]
        account = self._submission_dict["account"]
        queue = self._submission_dict["queue"]
        mail = self._submission_dict["mail"]
        # Notice that we do not add the stem here
        self._log_and_error_base = self.store_dir.joinpath(self._job_name)

        waiting_for_str = (
            f"#SBATCH --dependency=afterok:{':'.join(waiting_for)}{newline}"
            if len(waiting_for) != 0
            else ""
        )
        job_string = (
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self._job_name}\n"
            f"#SBATCH --nodes={self.processor_split.number_of_nodes}\n"
            f"#SBATCH --tasks-per-node={self.processor_split.processors_per_node}\n"
            # d-hh:mm:ss
            f"{f'#SBATCH --time={walltime}{newline}' if walltime is not None else ''}"
            f"{f'#SBATCH --account={account}{newline}' if account is not None else ''}"
            f"{f'#SBATCH -p {queue}{newline}' if queue is not None else ''}"
            f"#SBATCH -o {self._log_and_error_base}.log\n"
            f"#SBATCH -e {self._log_and_error_base}.err\n"
            f"{f'#SBATCH --mail-type=ALL{newline}' if mail is not None else ''}"
            f"{f'#SBATCH --mail-user={mail}{newline}' if mail is not None else ''}"
            f"{waiting_for_str}"
            "\n"
            # Change directory to the directory of this script
            "cd $SLURM_SUBMIT_DIR\n"
            f"{command}"
        )

        return job_string

    def get_sacct(self) -> str:
        """
        Return the result from ``sacct``.

        Returns
        -------
        sacct_str : str
            The string obtained from ``sacct``
            An empty string is will be returned if no job_id exist
        """
        if self._status["job_id"] is not None:
            # Submit the command through a local submitter
            local_submitter = LocalSubmitter(run_path=self.store_dir)
            local_submitter.submit_command(
                f"sacct "
                f"--starttime {self.__sacct_starttime} "
                f"--j {self._status['job_id']} "
                f"--brief"
            )
            local_submitter.wait_until_completed()
            sacct_str = (
                local_submitter.std_out if local_submitter.std_out is not None else ""
            )
            return sacct_str
        return ""

    def reset(self) -> None:
        """Reset released, waiting_for and status dict."""
        self._released = False
        self._waiting_for = list()
        self._reset_status()
