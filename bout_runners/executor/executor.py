"""Contains the executor class."""


from bout_runners.make.make import MakeProject
from bout_runners.executor.run_parameters import RunParameters


class Executor:
    """
    Executes the command for submitting a run.

    FIXME: Add variables and attributes

    Examples
    --------
    FIXME: Update
    >>> bout_paths = BoutPaths('path/to/project/root')
    >>> runner = Executor(bout_paths)
    >>> runner.execute()
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
        run_parameters : RunParameters
            Object containing the run parameters
        submitter : AbstractSubmitter
            Object containing the submitter
        """
        # Set member data
        self.__bout_paths = bout_paths
        self.__run_parameters = run_parameters
        self.__make = MakeProject(self.__bout_paths.project_path)
        self.__command = self.get_execute_command()
        self.submitter = submitter

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
        self.submitter.submit.submit_command(self.__command)
