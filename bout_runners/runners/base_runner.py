"""Contains the base runner."""


import logging
import shutil
from datetime import datetime
from pathlib import Path
from bout_runners.bookkeeper.bookkeeper import Bookkeeper
from bout_runners.bookkeeper.bookkeeper_utils import get_db_path
from bout_runners.bookkeeper.bookkeeper_utils import tables_created
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

    def __init__(self,
                 project_path=None,
                 database_root_path=None):
        """
        Set the execution path and create the database.

        Parameters
        ----------
        project_path : None or Path or str
            Root path of make file
            If None, the path of the path of the root caller will be
            used
        database_root_path : None or Path or str
            Root path of the database file
            If None, the path will be set to $HOME/BOUT_db
        """
        # FIXME: The constructor is not cleaned after refactoring the
        #  classes
        if project_path is None:
            project_path = get_caller_dir()
        self.project_path = project_path
        logging.debug('self.project_path set to %s', project_path)

        # Get database
        db_path = get_db_path(project_path=project_path,
                              database_root_path=database_root_path)
        self.bookkeeper = Bookkeeper(db_path)

        self.make = None  # Set to make-obj in self.make_project
        self.bout_inp_src_dir = None  # Set to Path in self.set_bout_inp_src
        self.bout_inp_dst_dir = None  # Set to Path in self.set_destination
        self.nproc = None  # Set in set_split
        self.parameter_dict = None  # Set in self.set_parameter_dict
        self.options_str = None  # Set in self.set_parameter_dict

    def set_split(self, nproc):
        """
        Set the split.

        Parameters
        ----------
        nproc : int
            Total numbers of processors to use
        """
        self.nproc = nproc

    def set_parameter_dict(self, parameter_dict):
        """
        Set the parameter_dict.

        The parameter_dict set here will override those found in the
        BOUT.inp file

        Parameters
        ----------
        parameter_dict : dict of str, dict
            Options on the form
            >>> {'global':{'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}
        """
        self.parameter_dict = parameter_dict
        sections = list(self.parameter_dict.keys())

        # Generate the string
        self.options_str = ''
        if 'global' in sections:
            sections.remove('global')
            global_option = self.parameter_dict['global']
            for key, val in global_option.items():
                self.options_str += f'{key}={val} '

        for section in sections:
            for key, val in parameter_dict.items():
                self.options_str += f'{section}.{key}={val} '

    def make_project(self):
        """Set the make object and Make the project."""
        self.make = MakeProject(self.project_path)
        self.make.run_make()

    def run(self):
        """Execute a BOUT++ run."""
        # Make the project if not already made
        self.make_project()

        # Set the bout_inp_dst_dir if not set
        if self.bout_inp_dst_dir is None:
            self.set_destination()

        mpi_cmd = 'mpirun -np'
        nproc_str = self.nproc if self.nproc is not None else '1'
        dst_str = f' -d {self.bout_inp_dst_dir}'
        options_str = f' {self.options_str}' \
            if self.options_str is not None else ''

        # NOTE: No spaces if parameters are None
        command = f'{mpi_cmd} {nproc_str} ./{self.make.exec_name}' \
                  f'{dst_str}{options_str}'

        db_ready = tables_created(self.bookkeeper)
        if db_ready:
            self.bookkeeper.store_data_from_run(self, int(nproc_str))
        else:
            logging.warning('Database %s has no entries and is not '
                            'ready. '
                            'No data capture will be made.',
                            self.bookkeeper.database_path)

        run_subprocess(command, path=self.project_path)
        if db_ready:
            self.bookkeeper.update_status()


class DomainSplit:
    pass


class RunParameters:
    pass


# FIXME: You are here: Use this class as input to BoutRunner
class BoutPaths:
    """
    Class which sets the paths

    Attributes
    ----------

    Methods
    -------

    Examples
    --------
    FIXME
    """

    def __init__(self,
                 project_path=None,
                 bout_inp_src_dir=None,
                 bout_inp_dst_dir=None):
        """
        Sets the paths.

        Parameters
        ----------
        project_path : None or Path or str
            Root path of make file
            If None, the path of the path of the root caller will be
            used
        bout_inp_src_dir : None or str or Path
            The path to the BOUT.inp source directory (relative to
            self.project_path)
            If None, data will be used
        bout_inp_dst_dir : None or str or Path
            The path to the BOUT.inp bout_inp_dst_dir directory (relative to
            self.project_path)
            If None, the current time will be used
        """
        # Declare variables to be used in the getters and setters
        self.__project_path = None
        self.__bout_inp_src_dir = None
        self.__bout_inp_dst_dir = None

        # Set the project path
        self.project_path = project_path

        # Set the bout_inp_src_dir
        self.bout_inp_src_dir = bout_inp_src_dir

        # Set the bout_inp_dst_dir
        self.bout_inp_dst_dir = bout_inp_dst_dir

    @property
    def project_path(self):
        """
        Set properties of self.project_path

        Parameters
        ----------
        project_path : None or Path or str
            Root path of make file
            If None, the path of the path of the root caller will be
            used

        Returns
        -------
        self.__project_path : Path
            Absolute path to the root of make file
        """
        return self.__project_path

    @project_path.setter
    def project_path(self, project_path):
        if project_path is None:
            project_path = get_caller_dir()
        project_path.absolute()
        self.__project_path = project_path
        logging.debug('self.project_path set to %s', project_path)

    @property
    def bout_inp_src_dir(self):
        """
        Set properties of bout_inp_src_dir.

        The setter will convert bout_inp_src_dir an absoulte path
        (as the input is relative to the project path), check that
        the directory exists, and copy the BOUT.inp file to the
        bout_inp_dst_dir path (self.bout_inp_dst_dir)

        Parameters
        ----------
        bout_inp_src_dir : None or str or Path
            The path to the BOUT.inp source directory (relative to
            self.project_path)
            If None, data will be used

        Returns
        -------
        self.__bout_inp_src_dir : Path
            The absolute path to the BOUT.inp source directory
        """
        return self.__bout_inp_src_dir

    @bout_inp_src_dir.setter
    def bout_inp_src_dir(self, bout_inp_src_dir):
        bout_inp_src_dir = \
            Path(bout_inp_src_dir) if bout_inp_src_dir is not None\
            else Path('data')

        self.__bout_inp_src_dir = \
            self.project_path.joinpath(bout_inp_src_dir)

        if not bout_inp_src_dir.is_file:
            raise FileNotFoundError(f'{self.__bout_inp_src_dir} is not'
                                    f' a file')

        logging.debug('self.bout_inp_src_dir set to %s',
                      self.__bout_inp_src_dir)

        # Copy file to bout_inp_dst_dir if set
        if self.bout_inp_dst_dir is not None:
            self._copy_inp()

    @property
    def bout_inp_dst_dir(self):
        """
        Set properties of bout_inp_dst_dir.

        The setter will convert bout_inp_dst_dir an absoulte path
        (as the input is relative to the project path), and copy
        BOUT.inp from self.bout_inp_src_dir to self.bout_inp_dst_dir

        Parameters
        ----------
        bout_inp_dst_dir : None or str or Path
            The path to the BOUT.inp bout_inp_dst_dir directory (relative to
            self.project_path)
            If None, the current time will be used

        Returns
        -------
         self.__bout_inp_dst_dir : Path

        """
        return self.__bout_inp_dst_dir

    @bout_inp_dst_dir.setter
    def bout_inp_dst_dir(self, bout_inp_dst_dir):
        time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        bout_inp_dst_dir = \
            Path(bout_inp_dst_dir) if bout_inp_dst_dir is not None \
            else Path(time)

        self.__bout_inp_dst_dir = \
            self.project_path.joinpath(bout_inp_dst_dir)
        self.__bout_inp_dst_dir.mkdir(exist_ok=True, parents=True)
        logging.debug('self.bout_inp_dst_dir set to %s',
                      self.__bout_inp_dst_dir)

        self._copy_inp()

    def _copy_inp(self):
        """Copy BOUT.inp from source to bout_inp_dst_dir."""
        if self.bout_inp_src_dir != self.bout_inp_dst_dir:
            src = self.bout_inp_src_dir.joinpath('BOUT.inp')
            dst = self.bout_inp_dst_dir.joinpath(src.name)
            shutil.copy(src, dst)
            logging.debug('Copied %s to %s', src, dst)
