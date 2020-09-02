"""Contains the executor class."""


from typing import Optional

from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.make.make import Make
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.submitter.local_submitter import LocalSubmitter


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
    __command : str
        The terminal command for executing the run
    submitter : AbstractSubmitter
        Object containing the submitter
    bout_paths : BoutPaths
        Object containing the paths

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
        submitter: Optional[LocalSubmitter] = None,
        run_parameters: Optional[RunParameters] = None,
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
        """
        # Set member data
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.submitter = submitter if submitter is not None else LocalSubmitter()
        self.__bout_paths = bout_paths if bout_paths is not None else BoutPaths()
        self.__run_parameters = (
            run_parameters if run_parameters is not None else RunParameters()
        )
        self.__make = Make(self.__bout_paths.project_path)
        self.__command = self.get_execute_command()

    @property
    def bout_paths(self) -> BoutPaths:
        """
        Set the properties of self.bout_paths.

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
    def run_parameters(self):
        """
        Set the properties of self.run_parameters.

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
            f"./{self.__make.exec_name} "
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
        command = self.__command
        if restart:
            command += " restart"
        self.submitter.submit_command(command)
