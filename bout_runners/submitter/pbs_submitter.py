"""Contains the PBS submitter class."""


import logging
import re
from pathlib import Path
from time import sleep
from typing import Dict, Iterable, List, Optional, Tuple, Union

from bout_runners.submitter.abstract_submitters import (
    AbstractClusterSubmitter,
    AbstractSubmitter,
)
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit


class PBSSubmitter(AbstractSubmitter, AbstractClusterSubmitter):
    """
    The PBS submitter class.

    Attributes
    ----------
    __waiting_for : tuple of str
        Getter variable for waiting_for
    __log_and_error_base : Path
        Base for the path for the .log and .err files
    __dequeued : bool
        Whether or not the job has been dequeued from the queue
    released : bool
        Whether or not the job has been released to the queue
    waiting_for : tuple of str
        Tuple of job names which this job is waiting for

    Methods
    -------
    __get_trace()
        Return the trace from ``tracejob``
    _wait_for_std_out_and_std_err()
        Wait until the process completes if a process has been started
    get_return_code(trace)
        Return the exit code if any
    has_dequeue(trace)
        Return whether or not the job has been removed from the queue
    structure_time_to_pbs_format(time_str)
        Structure the time string to a PBS time string
    add_waiting_for(waiting_for_id)
        Add a waiting for id to the waiting for list
    kill()
        Kill a job
    release()
        Release job if held
    reset()
        Reset dequeued, released, waiting_for and status dict
    submit_command(command)
        Submit a command
    completed()
        Return the completed status
    raise_error()
        Raise and error from the subprocess in a clean way
    create_submission_string(command, waiting_for)
        Return the PBS script as a string
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
        # https://stackoverflow.com/questions/9575409/calling-parent-class-init-with-multiple-inheritance-whats-the-right-way/50465583
        AbstractSubmitter.__init__(self, processor_split)
        AbstractClusterSubmitter.__init__(
            self, job_name, store_directory, submission_dict
        )
        if self._submission_dict["walltime"] is not None:
            self._submission_dict["walltime"] = self.structure_time_to_pbs_format(
                self._submission_dict["walltime"]
            )
        self.__waiting_for: List[str] = list()
        self.__log_and_error_base: Path = Path()
        self.__dequeued = False

    def __get_trace(self) -> str:
        """
        Return the trace from ``tracejob``.

        Returns
        -------
        trace : str
            Trace obtained from the `tracejob`
        """
        # Submit the command through a local submitter
        local_submitter = LocalSubmitter(run_path=self.store_dir)
        local_submitter.submit_command(f"tracejob -n 365 {self._status['job_id']}")
        local_submitter.wait_until_completed()
        trace = local_submitter.std_out if local_submitter.std_out is not None else ""
        return trace

    def _wait_for_std_out_and_std_err(self) -> None:
        """
        Wait until the process completes if a process has been started.

        Populate return_code, std_out and std_err
        """
        if self._status["job_id"] is not None:
            self.release()
            while self._status["return_code"] is None and not self.__dequeued:
                trace = self.__get_trace()
                self._status["return_code"] = self.get_return_code(trace)
                self.__dequeued = self.has_dequeue(trace)
                sleep(5)
                logging.debug("Trace is reading:\n%s", trace)

            if self._status["return_code"] is not None:
                log_path = self.__log_and_error_base.parent.joinpath(
                    f"{self.__log_and_error_base.stem}.log"
                )
                with log_path.open("r") as file:
                    self._status["std_out"] = file.read()

                err_path = self.__log_and_error_base.parent.joinpath(
                    f"{self.__log_and_error_base.stem}.err"
                )
                with err_path.open("r") as file:
                    self._status["std_err"] = file.read()

                logging.debug(
                    "std_out and std_err populated for job_id %s (%s)",
                    self._status["job_id"],
                    self.job_name,
                )

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
    def waiting_for(self) -> Tuple[str, ...]:
        """
        Return the waiting for list as a tuple.

        Returns
        -------
        tuple of str
            The waiting for list as a tuple
        """
        return tuple(self.__waiting_for)

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
                self.__waiting_for.append(waiting_for_id)
                logging.debug(
                    "Adding the following to the waiting_for_list for %s: %s",
                    self.job_name,
                    waiting_for_id,
                )
            else:
                for waiting_id in waiting_for_id:
                    self.__waiting_for.append(waiting_id)
                    logging.debug(
                        "Adding the following to the waiting_for_list for %s: %s",
                        self.job_name,
                        waiting_id,
                    )

    def kill(self) -> None:
        """Kill a job."""
        if self.job_id is not None and not self.completed():
            logging.info("Killing job_id %s (%s)", self.job_id, self.job_name)
            submitter = LocalSubmitter()
            submitter.submit_command(f"qdel {self.job_id}")
            submitter.wait_until_completed()
            self._released = True

    def release(self) -> None:
        """Release job if held."""
        if self.job_id is not None and not self._released:
            logging.debug("Releasing job_id %s (%s)", self.job_id, self.job_name)
            submitter = LocalSubmitter()
            submitter.submit_command(f"qrls {self.job_id}")
            submitter.wait_until_completed()
            self._released = True

    def reset(self) -> None:
        """Reset dequeued, released, waiting_for and status dict."""
        self.__dequeued = False
        self._released = False
        self.__waiting_for = list()
        self._reset_status()

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
        local_submitter.submit_command(f"qsub -h {script_path}")
        local_submitter.wait_until_completed()
        self._status["job_id"] = local_submitter.std_out
        logging.info(
            "job_id %s (%s) given to command '%s' in %s",
            self.job_id,
            self.job_name,
            command,
            script_path,
        )

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
            trace = self.__get_trace()
            return_code = self.get_return_code(trace)
            if return_code is not None:
                self._status["return_code"] = return_code
                self._wait_for_std_out_and_std_err()
                return True
            self.__dequeued = self.has_dequeue(trace)
            if self.__dequeued:
                return True
        return False

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
            f"#PBS -o {self.__log_and_error_base}.log\n"
            f"#PBS -e {self.__log_and_error_base}.err\n"
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
