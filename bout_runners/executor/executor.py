"""Contains the executor class."""


from bout_runners.make.make import Make
from bout_runners.parameters.run_parameters import RunParameters


class Executor:
    r"""
    Executes the command for submitting a run.

    Attributes
    ----------
    __bout_paths : BoutPaths
        Getter and setter variable for project_path
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
    Import the dependencies
    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.submitter.local_submitter import \
    ...     LocalSubmitter

    Create the `bout_paths` object
    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source',
    ... 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination',
    ... 'BOUT.inp')
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

    def __init__(self,
                 bout_paths,
                 submitter,
                 run_parameters=RunParameters()):
        """
        Set the input parameters.

        Parameters
        ----------
        bout_paths : BoutPaths
            Object containing the paths
        submitter : AbstractSubmitter
            Object containing the submitter
        run_parameters : RunParameters
            Object containing the run parameters
        """
        # Set member data
        self.submitter = submitter
        self.__bout_paths = bout_paths
        self.__run_parameters = run_parameters
        self.__make = Make(self.__bout_paths.project_path)
        self.__command = self.get_execute_command()

    @property
    def bout_paths(self):
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

    @bout_paths.setter
    def bout_paths(self, _):
        msg = (f'The bout_paths is read only, and is '
               f'set through the constructor')
        raise AttributeError(msg)

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

    @run_parameters.setter
    def run_parameters(self, _):
        msg = (f'The run_parameters is read only, and is '
               f'set through the constructor')
        raise AttributeError(msg)

    def get_execute_command(self):
        """
        Return the execute command string.

        Returns
        -------
        command : str
            The terminal command for executing the run
        """
        mpi_cmd = 'mpirun -np'

        # NOTE: No spaces if parameters are None
        command = \
            (f'{mpi_cmd} '
             f'{self.submitter.processor_split.number_of_processors} '
             f'./{self.__make.exec_name} '
             f'-d {self.__bout_paths.bout_inp_dst_dir} '
             f'{self.__run_parameters.run_parameters_str}')
        return command

    def execute(self):
        """Execute a BOUT++ run."""
        # Make the project if not already made
        self.__make.run_make()
        # Submit the command
        self.submitter.submit_command(self.__command)
