"""Contains the base runner."""


import logging
import shutil
from datetime import datetime
from pathlib import Path
from bout_runners.make.make import MakeProject
from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.utils.subprocesses_functions import run_subprocess


class BoutRunner:
    """
    The basic runner class.

    Examples
    --------
    >>> from bout_runners.runners.base_runner import BoutRunner
    >>> runner = BoutRunner('path/to/project/root')
    >>> runner.run()
    """

    def __init__(self, execute_from_path):
        """
        Set the execution path.

         Parameters
        ----------
        execute_from_path : None or Path or str
            Root path of make file
            If None, the path of the path of the root caller will be
            used
        """
        if execute_from_path is None:
            execute_from_path = get_caller_dir()
        self.execute_from_path = execute_from_path
        logging.debug(f'self.execute_from_path set '
                      f'to {execute_from_path}')

        self.make = None  # Set to make-obj in self.make
        self.bout_inp_src = None  # Set to Path in self.set_bout_inp_src
        self.destination = None  # Set to Path in self.set_destination
        self.nproc = None  # Set in set_split
        self.options = None  # Set in self.set_options
        self.options_str = None  # Set in self.set_options

    def _copy_inp(self):
        """Copy BOUT.inp from source to destination."""
        if not self.bout_inp_src == self.destination:
            src = self.bout_inp_src.joinpath('BOUT.inp')
            dst = self.destination.joinpath(src.name)
            shutil.copy(src, dst)
            logging.debug(f'Copied {src} to {dst}')

    def set_inp_src(self, inp_path=None):
        """
        Set the path to the directory of the BOUT.inp source

        Parameters
        ----------
        inp_path : None or str or Path
            The path to BOUT.inp (relative to self.execute_from_path)
            If None, data/BOUT.inp will be used
        """
        bout_inp_src = \
            Path(inp_path) if inp_path is not None else Path('data')

        self.bout_inp_src = \
            self.execute_from_path.joinpath(bout_inp_src)

        if not self.bout_inp_src.is_file:
            raise FileNotFoundError(f'{self.bout_inp_src} is not a '
                                    f'file')

        logging.debug(f'self.bout_inp_src set to {self.bout_inp_src}')

        # Copy file to destination if set
        if self.destination is not None:
            self._copy_inp()

    def set_destination(self, dst_path=None):
        """
        Set the destination of the run files.

        The BOUT.inp is copied from self.bout_inp_src to the destination

        Parameters
        ----------
        dst_path : None or str or Path
            The path to the destination (relative to
            self.execute_from_path)
            If None,the date will be set
        """
        time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        dst_path = \
            Path(dst_path) if dst_path is not None else Path(time)

        self.destination = self.execute_from_path.joinpath(dst_path)
        self.destination.mkdir(exist_ok=True, parents=True)
        logging.debug(f'self.destination set to {self.destination}')

        self._copy_inp()

    def set_split(self, nproc):
        """
        Set the split.

        Parameters
        ----------
        nproc : int
            Total numbers of processors to use
        """
        self.nproc = nproc

    def set_options(self, options):
        """
        Set the options.

        The options set here will override those found in the
        BOUT.inp file

        Parameters
        ----------
        options : dict of str, dict
            Options on the form
            >>> {'global':{'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}
        """
        self.options = options
        sections = list(self.options.keys())

        # Generate the string
        self.options_str = ''
        if 'global' in sections:
            sections.remove('global')
            global_option = self.options['global']
            for key, val in global_option.items():
                self.options_str += f'{key}={val} '

        for section in sections:
            for key, val in options.items():
                self.options_str += f'{section}.{key}={val} '

    def make_project(self):
        """Set the make object and Make the project."""
        self.make = MakeProject(self.execute_from_path)
        self.make.run_make()

    def run(self):
        """Execute a BOUT++ run."""
        # Make the project if not already made
        self.make_project()

        # Set the destination if not set
        if self.destination is None:
            self.set_destination()

        mpi_cmd = 'mpirun -np'
        nproc_str = self.nproc if self.nproc is not None else 1
        dst_str = f' -d {self.destination}'
        options_str = f' {self.options_str}' \
            if self.options_str is not None else ''

        # NOTE: No spaces if parameters are None
        command = f'{mpi_cmd} {nproc_str} ./{self.make.exec_name}' \
                  f'{dst_str}{options_str}'

        run_subprocess(command, path=self.execute_from_path)
