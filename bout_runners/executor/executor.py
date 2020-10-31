"""Contains the executor class."""


import re
import logging
from typing import Optional
from pathlib import Path
from copy import deepcopy

from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.make.make import Make
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from bout_runners.submitter.abstract_submitters import AbstractClusterSubmitter
from bout_runners.submitter.submitter_factory import get_submitter


class Executor:
    r"""
    Executes the command for submitting a run.

    Attributes
    ----------
    __bout_paths : BoutPaths
        Getter variable for project_path
    __run_parameters : RunParameters
        Object containing the run parameters
    __make : Make
        Object for making the project
    restart_from : None or Path
        Path to copy restart files from prior to the execution
    submitter : AbstractSubmitter
        Object containing the submitter
    exec_name : str
        Name of the executable
    bout_paths : BoutPaths
        Object containing the paths
    run_parameters : RunParameters
        Object containing the run parameters

    Methods
    -------
    get_execute_command()
        Return the execute command string
    execute()
        Execute a BOUT++ run

    Examples
    --------
    The easiest way to use the Executor is to run a script from the root directory of
    the project (i.e. where the `Makefile` and `data` directory are normally
    situated. The script can simply call

    >>> Executor().execute()

    and `Executor` takes care of the rest.

    A more elaborate example where all the dependency objects are built manually:

    Import the dependencies

    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.submitter.local_submitter import LocalSubmitter

    Create the `bout_paths` object

    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Create the executor object

    >>> run_parameters = RunParameters({'global': {'nout': 0}})
    >>> executor = Executor(
    ...     bout_paths=bout_paths,
    ...     submitter=LocalSubmitter(bout_paths.project_path),
    ...     run_parameters=run_parameters)

    Execute the run

    >>> executor.execute()
    """

    def __init__(
        self,
        bout_paths: Optional[BoutPaths] = None,
        submitter: Optional[AbstractSubmitter] = None,
        run_parameters: Optional[RunParameters] = None,
        restart_from: Optional[Path] = None,
    ) -> None:
        """
        Set the input parameters.

        Parameters
        ----------
        bout_paths : BoutPaths or None
            Object containing the paths
            If None, default BoutPaths values will be used
        submitter : AbstractSubmitter
            Object containing the submitter
        run_parameters : RunParameters or None
            Object containing the run parameters
            If None, default parameters will be used
        restart_from : Path or None
            The path to copy the restart files from
        """
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        # NOTE: We are deepcopying bout_paths as it may be altered by for
        #       example the self.restart_from setter
        logging.info("Start: Making an Executor object")
        self.__bout_paths = (
            deepcopy(bout_paths) if bout_paths is not None else BoutPaths()
        )
        self.__run_parameters = (
            run_parameters if run_parameters is not None else RunParameters()
        )
        self.__make = Make(self.__bout_paths.project_path)

        self.submitter = submitter if submitter is not None else get_submitter()
        if isinstance(self.submitter, AbstractClusterSubmitter):
            self.submitter.store_dir = self.__bout_paths.bout_inp_dst_dir

        self.__restart_from = None
        self.restart_from = restart_from
        logging.info("Done: Making an Executor object")

    @property
    def restart_from(self) -> Optional[Path]:
        """
        Set the properties of self.restart from and update bout_inp_dst_dir.

        The bout_inp_dst_dir is updated to reflect that this is a restart run.

        The new bout_inp_dst_dir will be the same as
        bout_run_setup.executor.restart_from with _restart_/d* appended
        /d* will be the next digit based on the number of other restart directories

        Notes
        -----
        This will not copy the restart files as the restart files may not be ready.
        Copying of files can either be done manually using
        bout_runner.utils.file_operations.copy_restart_files or automatically by
        using BoutRunner.__setup_restart_files which is called from
        BoutRunner.__prepare_run

        See Also
        --------
        BoutRunner.__setup_restart_files(node_with_restart)
            Search for restart files, make a restart node where needed
        """
        return self.__restart_from

    @restart_from.setter
    def restart_from(self, restart_from: Optional[Path]) -> None:
        self.__restart_from = restart_from
        if restart_from is not None:
            logging.info(
                "Changing bout_paths.bout_inp_dst_dir as restart_from is not None"
            )
            restart_dir_parent = restart_from.parent
            restart_dir_name = restart_from.name
            restart_dirs = list(restart_dir_parent.glob(f"{restart_dir_name}*"))
            restart_number = 0
            restart_numbers = list()
            pattern = r"_restart_(\d)+$"
            for restart_dir in restart_dirs:
                match = re.search(pattern, restart_dir.name)
                if match is not None:
                    # NOTE: The zeroth group is the matching string
                    restart_numbers.append(int(match.group(1)))
            if len(restart_numbers) != 0:
                restart_numbers.sort()
                restart_number = restart_numbers[-1] + 1
            prev_inp_dst_dir = self.bout_paths.bout_inp_dst_dir
            stripped_restart_dir_name = re.sub(pattern, "", restart_dir_name)
            new_inp_dst_dir = restart_dir_parent.joinpath(
                f"{stripped_restart_dir_name}_restart_{restart_number}"
            )
            self.bout_paths.bout_inp_dst_dir = new_inp_dst_dir
            logging.info(
                "bout_run_setup.bout_paths.bout_inp_dst_dir set from %s to %s",
                prev_inp_dst_dir,
                new_inp_dst_dir,
            )

    @property
    def bout_paths(self) -> BoutPaths:
        """
        Get the properties of self.bout_paths.

        Returns
        -------
        self.__bout_paths : BoutPaths
            Object containing the paths

        Notes
        -----
        The bout_paths is read only
        """
        return self.__bout_paths

    @property
    def exec_name(self) -> str:
        """
        Set the properties of self.bout_paths.

        Returns
        -------
        self.__bout_paths : BoutPaths
            Object containing the paths

        Notes
        -----
        The exec_name is read only
        """
        return self.__make.exec_name

    @property
    def run_parameters(self) -> RunParameters:
        """
        Get the properties of self.run_parameters.

        Returns
        -------
        self.__run_parameters : RunParameters
            Object containing the run parameters

        Notes
        -----
        The run_parameters is read only
        """
        return self.__run_parameters

    def get_execute_command(self) -> str:
        """
        Return the execute command string.

        Returns
        -------
        command : str
            The terminal command for executing the run
        """
        mpi_cmd = "mpirun -np"

        # NOTE: No spaces if parameters are None
        command = (
            f"{mpi_cmd} "
            f"{self.submitter.processor_split.number_of_processors} "
            f"{self.__bout_paths.project_path.joinpath(self.exec_name)} "
            f"-d {self.__bout_paths.bout_inp_dst_dir} "
            f"{self.__run_parameters.run_parameters_str}"
        )
        return command

    def execute(self, restart: bool = False) -> None:
        """
        Execute a BOUT++ run.

        Parameters
        ----------
        restart : bool
            If True the 'restart' will be appended to the command string
        """
        # Make the project if not already made
        self.__make.run_make()
        # Submit the command
        command = self.get_execute_command()
        if restart:
            command += " restart"
        self.submitter.submit_command(command)
