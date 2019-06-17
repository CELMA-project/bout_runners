import os
import sys
import re
import itertools
import glob
import timeit
import datetime
import time
import shutil
from numbers import Number
import numpy as np
from boututils.run_wrapper import shell, launch, getmpirun
from boututils.options import BOUTOptions
from boututils.datafile import DataFile
from boutdata.restart import redistribute, addnoise, resizeZ, resize


class PBS_runner(basic_runner):
    """
    pbs_runner
    ----------

    Class for mpi running one or several runs with BOUT++.
    Works like the basic_runner, but submits the jobs to a Portable
    Batch System (PBS).

    For the additional member data, see the docstring of __init__.

    For more info check the docstring of bout_runners.
    """

# The constructor
    def __init__(self,
                 BOUT_nodes=1,
                 BOUT_ppn=1,
                 BOUT_walltime=None,
                 BOUT_queue=None,
                 BOUT_mail=None,
                 BOUT_run_name=None,
                 BOUT_account=None,
                 post_process_nproc=None,
                 post_process_nodes=None,
                 post_process_ppn=None,
                 post_process_walltime=None,
                 post_process_queue=None,
                 post_process_mail=None,
                 post_process_run_name=None,
                 post_process_account=None,
                 **kwargs):
        """
        PBS_runner constructor
        ----------------------

        All the member data is set to None by default, with the
        exception of BOUT_nodes (default=1) and BOUT_ppn (default = 4).

        Parameters
        ----------

        BOUT_nodes : int
            Number of nodes for one submitted BOUT job
        BOUT_ppn : int
            Processors per node for one submitted BOUT job
        BOUT_walltime : str
            Maximum wall time for one submitted BOUT job
        BOUT_queue : str
            The queue to submit the BOUT jobs
        BOUT_mail : str
            Mail address to notify when a BOUT job has finished
        BOUT_run_name : str
            Name of the BOUT run on the cluster (optional)
        BOUT_account : str
            Account number to use for the run (optional)
        post_process_nproc : int
            Total number of processors for one submitted post processing
            job
        post_process_nodes : int
            Number of nodes for one submitted post processing job
        post_process_ppn : int
            Processors per node for one submitted BOUT job
        post_process_walltime : str
            Maximum wall time for one submitting post processing job
        post_process_queue : str
            The queue to submit the post processing jobs
        post_process_mail : str
            Mail address to notify when a post processing job has
            finished
        post_process_run_name : str
            Name of the post processing run on the cluster (optional)
        post_process_account : str
            Account number to use for the post processing (optional)
        **kwargs : any
            As the constructor of bout_runners is called, this
            additional keyword makes it possible to specify the member
            data of bout_runners in the constructor of PBS_runner (i.e.
            nprocs = 1 is an allowed keyword argument in the constructor
            of PBS_runner).

            For a full sequence of possible keywords, see the docstring of
            the bout_runners constructor.
        """

        # Note that the constructor accepts additional keyword
        # arguments (**kwargs). These must match the keywords of the
        # parent class "basic_runner", which is called by the "super"
        # function below

        # Call the constructor of the superclass
        super(PBS_runner, self).__init__(**kwargs)

        # Options set for the BOUT runs
        self._BOUT_nodes = BOUT_nodes
        self._BOUT_ppn = BOUT_ppn
        self._BOUT_walltime = BOUT_walltime
        self._BOUT_mail = BOUT_mail
        self._BOUT_queue = BOUT_queue
        self._BOUT_run_name = BOUT_run_name
        self._BOUT_account = BOUT_account
        # Options set for the post_processing runs
        self._post_process_nproc = post_process_nproc
        self._post_process_nodes = post_process_nodes
        self._post_process_ppn = post_process_ppn
        self._post_process_walltime = post_process_walltime
        self._post_process_mail = post_process_mail
        self._post_process_queue = post_process_queue
        self._post_process_run_name = post_process_run_name
        self._post_process_account = post_process_account

        # Options set for all runs
        self._run_type = "basic_PBS"

        # Error check the input data
        self._check_for_PBS_instance_error()

        # Initialize the jobid returned from the PBS
        self._PBS_id = []

# The run_driver
    def _run_driver(self, combination, run_no):
        """The machinery which actually performs the run"""

        # Submit the job to the queue
        self._single_submit(combination, run_no, append_to_run_log=True)

    def _check_for_PBS_instance_error(self):
        """Check if there are any type errors when creating the object"""

        # BOUT_ppn and BOUT_nodes are set by default, however, we must check
        # that the user has not given them as wrong input
        if not isinstance(self._BOUT_ppn, int):
            message = ("BOUT_ppn is of wrong type\n"
                       "BOUT_ppn must be given as a int")
            self._errors.append("TypeError")
            raise TypeError(message)
        if not isinstance(self._BOUT_nodes, int):
            message = ("BOUT_nodes is of wrong type\n"
                       "BOUT_nodes must be given as a int")
            self._errors.append("TypeError")
            raise TypeError(message)

        if self._nproc > (self._BOUT_nodes * self._BOUT_ppn):
            message = "Must have nproc <= BOUT_nodes * BOUT_ppn"
            self._errors.append("TypeError")
            raise TypeError(message)

        check_if_set = (
            self._post_process_nproc,
            self._post_process_nodes,
            self._post_process_ppn,
        )
        # All elements of check_if_set must be set if any is set
        not_None = 0
        for check in check_if_set:
            if check is not None:
                not_None += 1

        if (not_None != 0) and (not_None != len(check_if_set)):
            message = ("If any of post_process_nproc, post_process_nodes,"
                       " post_process_ppn and post_process_walltime is"
                       " set, all others must be set as well.")
            self._errors.append("TypeError")
            raise TypeError(message)

        check_if_int = (
            (self._post_process_nodes, "post_process_nodes"),
            (self._post_process_ppn, "post_process_ppn")
        )
        self._check_for_correct_type(var=check_if_int,
                                     the_type=int,
                                     allow_iterable=False)

        if self._post_process_nproc is not None:
            if self._post_process_nproc > \
                    (self._post_process_nodes * self._post_process_ppn):
                message = ("Must have post_process_nproc <= "
                           "post_process_nodes * post_process_ppn")
                self._errors.append("TypeError")
                raise TypeError(message)

        check_if_str = (
            (self._BOUT_walltime, "BOUT_walltime"),
            (self._BOUT_mail, "BOUT_mail"),
            (self._BOUT_queue, "BOUT_queue"),
            (self._BOUT_run_name, "BOUT_run_name"),
            (self._BOUT_account, "BOUT_account"),
            (self._post_process_walltime, "BOUT_walltime"),
            (self._post_process_mail, "post_process_mail"),
            (self._post_process_queue, "post_process_queue"),
            (self._post_process_run_name, "post_process_run_name"),
            (self._post_process_account, "post_process_account"),
        )
        self._check_for_correct_type(var=check_if_str,
                                     the_type=str,
                                     allow_iterable=False)

        # A list to loop over
        walltimes = []
        # Append the walltimes if set
        if self._BOUT_walltime is not None:
            walltimes.append((self._BOUT_walltime,
                              "BOUT_walltime"))
        if self._post_process_walltime is not None:
            walltimes.append((self._post_process_walltime,
                              "post_process_walltime"))

        # Loop over the walltimes
        for walltime in walltimes:
            # Set a flag which states whether or not the check was
            # successful
            success = True
            # Split the walltime string
            walltime_list = walltime[0].split(":")
            # Check that the list has three elements
            if len(walltime_list) == 3:

                # Check that seconds is on the format SS
                if len(walltime_list[2]) == 2:
                    # Check that the last element (seconds) is a digit (int)
                    if walltime_list[2].isdigit():
                        # Check that the element is less than 59
                        if int(walltime_list[2]) > 59:
                            success = False
                    # Seconds is not a digit
                    else:
                        success = False
                # Seconds is not on the format SS
                else:
                    success = False

                # Do the same for the second last element (minutes)
                if len(walltime_list[1]) == 2:
                    # Check that the last element (seconds) is a digit (int)
                    if walltime_list[1].isdigit():
                        if int(walltime_list[1]) > 59:
                            success = False
                    # Minutes is not a digit
                    else:
                        success = False
                # Seconds is not on the format SS
                else:
                    success = False

                # Check that the first element (hours) is a digit
                if not(walltime_list[0].isdigit()):
                    success = False

            # walltime_list does not have three elements
            else:
                success = False

            if not(success):
                message = walltime[1] + " must be on the form H...H:MM:SS"
                self._errors.append("TypeError")
                raise TypeError(message)

    def _check_for_child_class_errors(
        self,
        remove_old,
        post_processing_function,
        post_process_after_every_run
    ):
        """Function which check for errors in a child class."""

        # Check member data is set if post_processing_function is not None
        if post_processing_function is not None:
            check_if_set = (
                self._post_process_nproc,
                self._post_process_nodes,
                self._post_process_ppn,
            )
            # All elements of check_if_set must be set if any is set
            not_None = 0
            for check in check_if_set:
                if check is not None:
                    not_None += 1

            if (not_None != 0) and (not_None != len(check_if_set)):
                message = ("post_process_nproc, post_process_nodes,"
                           " and post_process_ppn and must"
                           " be set if post_processing_function is set.")
                self._errors.append("TypeError")
                raise TypeError(message)

    def _print_run_or_submit(self):
        """Prints "submitting" """
        print("\nSubmitting:")

    def _single_submit(self, combination, run_no, append_to_run_log=None):
        """Submit a single BOUT job and submit the jobid to self._PBS_id"""

        # Get the script (as a string) which is going to be
        # submitted
        job_string = self._get_job_string(run_no,
                                          combination,
                                          append_to_run_log)

        # The submission
        PBS_id = self._submit_to_PBS(job_string)
        self._PBS_id.append(PBS_id)

    def _call_post_processing_function(
        self,
        function=None,
        folders=None,
        **kwargs
    ):
        """
        Function which submits the post processing to the PBS

        This is done by making a self deleting temporary python file
        that will be called by a PBS script.
        """

        # Get the start_time (to be used in the name of the file)
        start_time = self._get_start_time()

        # The name of the file
        python_name = "tmp_{}_{}.py".format(function.__name__, start_time)

        # Make the script
        python_tmp  = "#!/usr/bin/env python3\n"
        python_tmp += "import os, sys\n"
        # Set the python path
        python_tmp += "sys.path = {}\n".format(sys.path)
        # Import the post processing function
        python_tmp += "from {} import {}\n".\
            format(function.__module__, function.__name__)
        # Convert the keyword args to proper arguments
        # Appendable list
        arguments = []
        for key in kwargs.keys():
            if not isinstance(kwargs[key], str):
                # If the value is not a string, we can append it directly
                arguments.append("{}={}".format(key, kwargs[key]))
            else:
                # If the value is a string, we need to put quotes around
                arguments.append("{}='{}'".format(key, kwargs[key]))

        # Put a comma in between the arguments
        arguments = ", ".join(arguments)
        # Call the post processing function
        if hasattr(folders, "__iter__") and not isinstance(folders, str):
            python_tmp += "{}({},{})\n".\
                format(function.__name__, tuple(folders), arguments)
        elif isinstance(folders, str):
            python_tmp += "{}(('{}',),{})\n".\
                format(function.__name__, folders, arguments)
        # When the script has run, it will delete itself
        python_tmp += "os.remove('{}')\n".format(python_name)

        # Write the python script
        with open(python_name, "w") as f:
            f.write(python_tmp)

        # Creating the job string
        if self._post_process_run_name is None:
            job_name = "post_process_{}_".format(function.__name__, start_time)
        else:
            job_name = self._post_process_run_name

        # Get core of the job string
        job_string = self._create_PBS_core_string(
            job_name=job_name,
            nodes=self._post_process_nodes,
            ppn=self._post_process_ppn,
            walltime=self._post_process_walltime,
            mail=self._post_process_mail,
            queue=self._post_process_queue,
            account=self._post_process_account,
        )
        # Call the python script in the submission

        job_string += "python {}\n".format(python_name)
        job_string += "exit"

        # Create the dependencies
        dependencies = ":".join(self._PBS_id)
        # Submit the job
        print("\nSubmitting the post processing function '{}'\n".
              format(function.__name__))
        self._submit_to_PBS(job_string, dependent_job=dependencies)

    def _get_job_string(self, run_no, combination, append_to_run_log):
        """
        Make a string which will saved as a shell script before being
        sent to the PBS queue.
        """

        # Split the name to a list
        combination_name = combination.split(" ")
        # Remove whitespace
        combination_name = tuple(element for element in combination_name
                                 if element != "")
        # Collect the elements
        combination_name = "_".join(combination_name)
        # Replace bad characters
        combination_name = combination_name.replace(":", "")
        combination_name = combination_name.replace("=", "-")

        # Name of job
        if self._BOUT_run_name is None:
            job_name = "{}_{}_{}".\
                       format(combination_name, self._directory, run_no)
        else:
            job_name = self._BOUT_run_name

        command = self._get_command_to_run(combination)
        command = "mpirun -np {} {}".format(self._nproc, command)

        # Print the command
        print(command + "\n")

        job_string = self._create_PBS_core_string(
            job_name=job_name,
            nodes=self._BOUT_nodes,
            ppn=self._BOUT_ppn,
            walltime=self._BOUT_walltime,
            mail=self._BOUT_mail,
            queue=self._BOUT_queue,
            account=self._BOUT_account,
        )

        if append_to_run_log:
            start = datetime.datetime.now()
            start_time = "{}-{}-{}-{}:{}:{}".\
                         format(start.year, start.month, start.day,
                                start.hour, start.minute, start.second)

            job_string += "start=`date +%s`\n"
            # Run the bout program
            job_string += command + "\n"
            # end the timer
            job_string += "end=`date +%s`\n"
            # Find the elapsed time
            job_string += "time=$((end-start))\n"
            # The string is now in seconds
            # The following procedure will convert it to H:M:S
            job_string += "h=$((time/3600))\n"
            job_string += "m=$((($time%3600)/60))\n"
            job_string += "s=$((time%60))\n"

            # Ideally we would check if any process were writing to
            # run_log.txt
            # This could be done with lsof command as described in
            # http://askubuntu.com/questions/14252/how-in-a-script-can-i-determine-if-a-file-is-currently-being-written-to-by-ano
            # However, lsof is not available on all clusters

            # Using the same formatting as in _append_run_log, we are going
            # to echo the following to the run_log when the run is finished
            job_string += "echo '" +\
                          "{:<19}".format(start_time) + " " * 3 +\
                          "{:^9}".format(self._run_type) + " " * 3 +\
                          "{:^6}".format(str(run_no)) + " " * 3 +\
                          "'$h':'$m':'$s" + " " * 10 +\
                          "{:<}".format(self._dmp_folder) + " " * 3 +\
                          " >> $PBS_O_WORKDIR/" + self._directory +\
                          "/run_log.txt\n"

        # Exit the qsub
        job_string += "exit"

        return job_string

    def _get_start_time(self):
        """
        Returns a string of the current time down to micro precision
        """

        # The time is going to be appended to the job name and python name
        # In case the process is really fast, so that more than one job
        # is submitted per second, we add a microsecond in the
        # names for safety
        time_now = datetime.datetime.now()
        start_time = "{}-{}-{}-{}".format(time_now.hour,
                                          time_now.minute,
                                          time_now.second,
                                          time_now.microsecond,
                                          )
        return start_time

    def _create_PBS_core_string(
        self,
        job_name=None,
        nodes=None,
        ppn=None,
        walltime=None,
        mail=None,
        queue=None,
        account=None,
    ):
        """
        Creates the core of a PBS script as a string
        """

        # Shebang line
        job_string = "#!/bin/bash\n"
        # The job name
        job_string += "#PBS -N {}\n".format(job_name)
        job_string += "#PBS -l nodes={}:ppn={}\n".format(nodes, ppn)
        # If walltime is set
        if walltime is not None:
            # Wall time, must be in format HOURS:MINUTES:SECONDS
            job_string += "#PBS -l walltime={}\n".format(walltime)
        # If submitting to a specific queue
        if queue is not None:
            job_string += "#PBS -q {}\n".format(queue)
        job_string += "#PBS -o {}.log\n".\
            format(os.path.join(self._dmp_folder, job_name))
        job_string += "#PBS -e {}.err\n".\
            format(os.path.join(self._dmp_folder, job_name))
        if account is not None:
            job_string += "#PBS -A {}\n".format(account)
        # If we want to be notified by mail
        if mail is not None:
            job_string += "#PBS -M {}\n".format(mail)
            # #PBS -m abe
            # a=aborted b=begin e=ended
            job_string += "#PBS -m e\n"
        # cd to the folder you are sending the qsub from
        job_string += "cd $PBS_O_WORKDIR\n"

        return job_string

    def _submit_to_PBS(self, job_string, dependent_job=None):
        """
        Saves the job_string as a shell script, submits it and deletes
        it. Returns the output from PBS as a string
        """

        # Create the name of the temporary shell script
        # Get the start_time used for the name of the script
        start_time = self._get_start_time()
        script_name = "tmp_{}.sh".format(start_time)

        # Save the string as a script
        with open(script_name, "w") as shell_script:
            shell_script.write(job_string)

        # Submit the jobs
        if dependent_job is None:
            # Without dependencies
            command = "qsub ./" + script_name
            status, output = shell(command, pipe=True)
        else:
            # If the length of the depend job is 0, then all the jobs
            # have completed, and we can carry on as usual without
            # dependencies
            if len(dependent_job) == 0:
                command = "qsub ./" + script_name
                status, output = shell(command, pipe=True)
            else:
                # With dependencies
                command = "qsub -W depend=afterok:{} ./{}".\
                          format(dependent_job, script_name)
                status, output = shell(command, pipe=True)

        # Check for success
        if status != 0:
            if status == 208:
                message = ("Runs finished before submission of the post"
                           " processing function. When the runs are done:"
                           " Run again with 'remove_old = False' to submit"
                           " the function.")
                self._warnings.append(message)
            else:
                print("\nSubmission failed, printing output\n")
                print(output)
                self._errors.append("RuntimeError")
                message = ("The submission failed with exit code {}"
                           ", see the output above").format(status)
                raise RuntimeError(message)

        # Trims the end of the output string
        output = output.strip(" \t\n\r")

        # Delete the shell script
        try:
            os.remove(script_name)
        except FileNotFoundError:
            # Do not raise an error
            pass

        return output


if __name__ == "__main__":

    print(("\n\nTo find out about the bout_runners, please read the user's "
           "manual, or have a look at 'BOUT/examples/bout_runners_example', "
           "or have a look at the documentation"))
