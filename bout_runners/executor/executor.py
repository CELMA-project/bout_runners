"""Contains the executor class."""


from bout_runners.make.make import MakeProject
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.executor.run_parameters import RunParameters
from bout_runners.utils.subprocesses_functions import run_subprocess


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
                 run_parameters=RunParameters(),
                 processor_split=ProcessorSplit()):
        """
        Set the input parameters.

        Parameters
        ----------
        bout_paths : bout_runners.runners.bout_paths.BoutPaths
            Class which contains the paths
        run_parameters : RunParameters
            Class containing the run parameters
        processor_split : ProcessorSplit
            Class containing the processor split
        """
        # Set member data
        self.bout_paths = bout_paths
        self.run_parameters = run_parameters
        self.processor_split = processor_split

        # FIXME: Use setters?
        self.make = None  # Set to make-obj in self.make_project
        self.command = self.get_execute_command()

    def make_project(self):
        """Set the make object and Make the project."""
        self.make = MakeProject(self.bout_paths.project_path)
        self.make.run_make()

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
        command = (f'{mpi_cmd} '
                   f'{self.processor_split.number_of_processors} '
                   f'./{self.make.exec_name} '
                   f'-d {self.bout_paths.bout_inp_dst_dir} '
                   f'{self.run_parameters.run_parameters_str}')
        return command

    def execute(self):
        """Execute a BOUT++ run."""
        # Make the project if not already made
        self.make_project()
        # FIXME: Use sumbitter class
        run_subprocess(self.command, path=self.bout_paths.project_path)
