"""Contains the SLURM submitter class."""


import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Dict, Optional, Tuple

from bout_runners.submitter.abstract_submitters import (
    AbstractClusterSubmitter,
    AbstractSubmitter,
)
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit


class SLURMSubmitter(AbstractSubmitter, AbstractClusterSubmitter):
    """
    The SLURM submitter class.

    FIXME: Attributes, methods, examples
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
        # https://stackoverflow.com/questions/9575409/calling-parent-class-init-with-multiple-inheritance-whats-the-right-way/50465583
        AbstractSubmitter.__init__(self, processor_split)
        AbstractClusterSubmitter.__init__(
            self, job_name, store_directory, submission_dict
        )
        if self._submission_dict["walltime"] is not None:
            self._submission_dict["walltime"] = self.structure_time_to_slurm_format(
                self._submission_dict["walltime"]
            )
        self.__log_and_error_base: Path = Path()
        self.__sacct_starttime = (datetime.now() - timedelta(days=365)).strftime(
            r"%Y-%d-%m"
        )
        self._cancel_str = "qdel"
        self._release_str = "qrls"
        self._submit_str = "qsub -h"

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
                (
                    self._status["std_out"],
                    self._status["std_err"],
                ) = self._get_std_out_and_std_err()

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
        Assumes the job line is the third line of sacct_str (
        the two others being headers)
        """
        job_lines = sacct_str.split("\n")
        if len(job_lines) >= 2:
            return None

        job_line = job_lines[2]
        pattern = r"(-?\d+):-?\d+$"
        # Using search as match will only search the beginning of
        # the string
        # https://stackoverflow.com/a/32134461/2786884
        match = re.search(pattern, job_line, flags=re.MULTILINE)
        if match is None:
            return None
        return int(match.group(1))

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
        self.__log_and_error_base = self.store_dir.joinpath(self._job_name)

        waiting_for_str = (
            f"#SBATCH --dependency=afterok:{':'.join(self.waiting_for)}{newline}"
            if len(self.waiting_for) != 0
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
            f"#SBATCH -o {self.__log_and_error_base}.log\n"
            f"#SBATCH -e {self.__log_and_error_base}.err\n"
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
