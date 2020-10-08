"""Contains the PBS submitter class."""

import logging

from pathlib import Path
from typing import Optional, Dict, List, Tuple

from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.abstract_submitters import (
    AbstractSubmitter,
    AbstractClusterSubmitter,
)


class PBSSubmitter(AbstractSubmitter, AbstractClusterSubmitter):
    """The PBS submitter class."""

    def __init__(
        self,
        job_name: str,
        store_path: Path,
        submission_dict: Optional[Dict[str, Optional[str]]] = None,
        processor_split: Optional[ProcessorSplit] = None,
    ):
        """
        Set the member data.

        # FIXME: self.__status from LocalSubmitter should be in the parent class

        Parameters
        ----------
        job_name : str
            Name of the job
        store_path : path
            Path to store the script
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
        super().__init__(job_name, store_path, submission_dict, processor_split)
        if self.__submission_dict["walltime"] is not None:
            self.__submission_dict["walltime"] = self.structure_time_to_pbs_format(
                self.__submission_dict["walltime"]
            )
        self.__waiting_for: List[str] = list()
        self.__submission_id: Optional[str] = None
        self.__pid: Optional[int] = None

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

    @property
    def waiting_for(self) -> Tuple[str, ...]:
        """
        Return the waiting for list as a tuple.

        Returns
        -------
        tuple of str
            The waiting for list as a tuple
        """
        return tuple(self.__waiting_for)

    def add_waiting_for(self, waiting_for_id: str) -> None:
        """
        Add a waiting for id to the waiting for list.

        This will waiting for list will be written to the submission string
        upon creation

        Parameters
        ----------
        waiting_for_id : str
            Id to the job waiting for
        """
        self.__waiting_for.append(waiting_for_id)

    def create_submission_string(self, command: str) -> str:
        """
        Create the core of a PBS script as a string.

        Parameters
        ----------
        command : str
            The command to submit

        Returns
        -------
        job_script : str
            The script to be submitted
        """
        # Backslash not allowed in f-string expression
        newline = "\n"
        walltime = self.__submission_dict["walltime"]
        account = self.__submission_dict["account"]
        queue = self.__submission_dict["queue"]
        mail = self.__submission_dict["mail"]
        log_and_error_base = self.__job_name
        waiting_for_str = (
            f"#PBS -W depend=afterok:{':'.join(self.waiting_for)}{newline}"
            if len(self.waiting_for) != 0
            else ""
        )
        job_string = (
            "#!/bin/bash\n"
            f"#PBS -N {self.__job_name}\n"
            f"#PBS -l nodes={self.__processor_split.number_of_nodes}"
            f":ppn={self.__processor_split.processors_per_node}\n"
            # hh:mm:ss
            f"{f'#PBS -l walltime={walltime}{newline}' if walltime is not None else ''}"
            f"{'#PBS -A {account}{newline}' if account is not None else ''}"
            f"{f'#PBS -q {queue}{newline}' if queue is not None else ''}"
            f"#PBS -o {log_and_error_base}.log\n"
            f"#PBS -e {log_and_error_base}.err\n"
            # a=abort b=begin e=end
            f"{'#PBS -m abe{newline}' if mail is not None else ''}"
            f"{'#PBS -M {mail}{newline}' if mail is not None else ''}"
            f"{waiting_for_str}"
            "\n"
            # Change directory to the directory of this script
            "cd $PBS_O_WORKDIR\n"
            f"{command}"
        )

        waiting_for_str = (
            f"#SBATCH --dependency=afterok:{':'.join(self.waiting_for)}{newline}"
            if len(self.waiting_for) != 0
            else ""
        )
        job_string = (
            "#!/bin/bash\n"
            f"#SBATCH --job-name={self.__job_name}\n"
            f"#SBATCH --nodes={self.__processor_split.number_of_nodes}\n"
            f"#SBATCH -n {self.__processor_split.processors_per_node}\n"
            # d-hh:mm:ss
            f"{'#SBATCH --time={walltime}{newline}}' if walltime is not None else ''}"
            f"{'#SBATCH --account={account}{newline}' if account is not None else ''}"
            f"{'#SBATCH -p {queue}{newline}' if queue is not None else ''}"
            f"#SBATCH -o {log_and_error_base}.log\n"
            f"#SBATCH -e {log_and_error_base}.err\n"
            f"{'#SBATCH --mail-type=ALL{newline}' if mail is not None else ''}"
            f"{'#SBATCH --mail-user={mail}{newline}' if mail is not None else ''}"
            f"{waiting_for_str}"
            "\n"
            # Change directory to the directory of this script
            "cd $SLURM_SUBMIT_DIR\n"
            f"{command}"
        )

        return job_string

    def submit_command(self, command: str) -> None:
        """
        Submit a command.

        FIXME: 4346 in bout-dev
        FIXME: Before sending command to this function one must inject the dependency

        Parameters
        ----------
        command : str
            Command to submit
        """
        script_path = self.__store_path.joinpath(f"{self.__job_name}.sh")
        with script_path.open("w") as file:
            file.write(self.create_submission_string(command))

        # Submit the command through a local submitter
        local_submitter = LocalSubmitter(run_path=self.__store_path)
        local_submitter.submit_command(f"qsub {script_path}")
        self.__pid = local_submitter.pid
        local_submitter.wait_until_completed()
        self.__submission_id = local_submitter.std_out

    @property
    def pid(self) -> Optional[int]:
        """
        Return the process id.

        FIXME: This could be removed as we are interested in submission id
               rather than pid
        """
        return self.__pid

    @property
    def submission_id(self) -> Optional[str]:
        """
        Return the submission id.

        Returns
        -------
        self.__submission_id : None or str
            The job id given the process by the cluster
            None is given if the process has not been submitted
        """
        return self.__submission_id

    @property
    def std_out(self) -> Optional[str]:
        """
        Return the standard output.

        FIXME: Can be the tail from the .log
        """

    @property
    def std_err(self) -> Optional[str]:
        """
        Return the standard error.

        FIXME: Can be the tail from the .err
        """

    @property
    def return_code(self) -> Optional[int]:
        r"""
        Return the return code.

        FIXME: Must be obtained from qstat or the like?
        FIXME: You are here: Use `jobtrace pbs_id` regex Exit_status=(\d*)
        https://stackoverflow.com/questions/24248735/how-get-information-of-completed-pbs-or-torque-jobs/35511337
        """

    def wait_until_completed(self, raise_error: bool = True) -> None:
        """
        Wait until the process has completed.

        FIXME

        Parameters
        ----------
        raise_error : bool
            Whether or not to raise errors
        """
        logging.critical("%s", raise_error)

    def completed(self) -> bool:
        """
        Return the completed status.

        Returns
        -------
        bool
            Whether the job has completed
        """
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
            Whether the job has errored
        """
        logging.critical("%s", raise_error)
        return True

    def raise_error(self) -> None:
        """Raise and error from the subprocess in a clean way."""
