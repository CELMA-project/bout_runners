"""Contains the PBS submitter class."""


import logging
import re
from pathlib import Path
from time import sleep
from typing import Dict, Optional, Tuple

from bout_runners.submitter.abstract_cluster_submitter import AbstractClusterSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit


class PBSSubmitter(AbstractClusterSubmitter):
    """
    The PBS submitter class.

    Attributes
    ----------
    __dequeued : bool
        Whether or not the job has been dequeued from the queue

    Methods
    -------
    _wait_for_std_out_and_std_err()
        Wait until the process completes if a process has been started
    get_return_code(sacct_str)
        Return the exit code if any
    get_return_code(trace)
        Return the exit code if any
    has_dequeue(trace)
        Return whether or not the job has been removed from the queue
    structure_time_to_pbs_format(time_str)
        Structure the time string to a PBS time string
    completed()
        Return the completed status
    create_submission_string(command, waiting_for)
        Return the PBS script as a string
    get_trace()
        Return the trace from ``tracejob``
    reset()
        Reset dequeued, released, waiting_for and status dict

    Examples
    --------
    >>> submitter = PBSSubmitter(job_name, store_path)
    >>> submitter.submit_command("echo 'Hello'")
    >>> submitter.wait_until_completed()
    >>> submitter.std_out
    Hello
    """

    def __init__(
        self,
        job_name: Optional[str] = None,
        store_directory: Optional[Path] = None,
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
        if self._submission_dict["walltime"] is not None:
            self._submission_dict["walltime"] = self.structure_time_to_pbs_format(
                self._submission_dict["walltime"]
            )
        self.__dequeued = False
        self._cluster_specific["cancel_str"] = "qdel"
        self._cluster_specific["release_str"] = "qrls"
        self._cluster_specific["submit_str"] = "qsub -h"

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
        return std_out

    def _wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes if a process has been started.

        Populate return_code, std_out and std_err
        """
        if self._status["job_id"] is not None:
            self.release()
            while self._status["return_code"] is None and not self.__dequeued:
                trace = self.get_trace()
                self._status["return_code"] = self.get_return_code(trace)
                self.__dequeued = self.has_dequeue(trace)
                sleep(5)
                logging.debug("Trace is reading:\n%s", trace)

            if self._status["return_code"] is not None:
                self._populate_std_out_and_std_err()
            else:
                # If the return code is empty it must be because the while loop
                # exited because the job was dequeued
                logging.error(
                    "The process dequeued before starting. "
                    "No process started, so "
                    "return_code, std_out, std_err are not populated"
                )
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
    def get_return_code(trace: str) -> Optional[int]:
        """
        Return the exit code if any.

        Parameters
        ----------
        trace : str
            Trace obtained from the `tracejob` command

        Returns
        -------
        return_code : None or int
            Return code obtained from the cluster
        """
        pattern = r"Exit_status=(\d*)"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, trace, flags=re.MULTILINE)
        if match is None:
            return None
        return int(match.group(1))

    @staticmethod
    def has_dequeue(trace: str) -> bool:
        """
        Return whether or not the job has been removed from the queue.

        Parameters
        ----------
        trace : str
            Trace obtained from the `tracejob` command

        Returns
        -------
        bool
            True if the job has been removed from the queue
        """
        pattern = r"dequeuing"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, trace, flags=re.MULTILINE)
        if match is None:
            return False
        return True

    @staticmethod
    def structure_time_to_pbs_format(time_str: str) -> str:
        """
        Structure the time string to a PBS time string.

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
        hours += days * 24
        return f"{hours}:{minutes}:{seconds}"

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
            trace = self.get_trace()
            return_code = self.get_return_code(trace)
            if return_code is not None:
                self._status["return_code"] = return_code
                self._wait_for_std_out_and_std_err()
                return True
            self.__dequeued = self.has_dequeue(trace)
            if self.__dequeued:
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
            f"#PBS -W depend=afterok:{':'.join(waiting_for)}{newline}"
            if len(waiting_for) != 0
            else ""
        )
        job_string = (
            "#!/bin/bash\n"
            f"#PBS -N {self._job_name}\n"
            f"#PBS -l nodes={self.processor_split.number_of_nodes}"
            f":ppn={self.processor_split.processors_per_node}\n"
            # hh:mm:ss
            f"{f'#PBS -l walltime={walltime}{newline}' if walltime is not None else ''}"
            f"{f'#PBS -A {account}{newline}' if account is not None else ''}"
            f"{f'#PBS -q {queue}{newline}' if queue is not None else ''}"
            f"#PBS -o {self._log_and_error_base}.log\n"
            f"#PBS -e {self._log_and_error_base}.err\n"
            # a=abort b=begin e=end
            f"{f'#PBS -m abe{newline}' if mail is not None else ''}"
            f"{f'#PBS -M {mail}{newline}' if mail is not None else ''}"
            f"{waiting_for_str}"
            "\n"
            # Change directory to the directory of this script
            "cd $PBS_O_WORKDIR\n"
            f"{command}"
        )
        return job_string

    def get_trace(self) -> str:
        """
        Return the trace from ``tracejob``.

        Returns
        -------
        trace : str
            Trace obtained from the ``tracejob``
            An empty string is will be returned if no job_id exist
        """
        if self._status["job_id"] is not None:
            # Submit the command through a local submitter
            local_submitter = LocalSubmitter(run_path=self.store_dir)
            local_submitter.submit_command(f"tracejob -n 365 {self._status['job_id']}")
            local_submitter.wait_until_completed()
            trace = (
                local_submitter.std_out if local_submitter.std_out is not None else ""
            )
            return trace
        return ""

    def reset(self) -> None:
        """Reset dequeued, released, waiting_for and status dict."""
        self._released = False
        self.__dequeued = False
        self._waiting_for = list()
        self._reset_status()
