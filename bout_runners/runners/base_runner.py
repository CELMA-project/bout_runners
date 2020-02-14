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


class BoutPaths:
    """
    Class which sets the paths.

    Attributes
    ----------
    __project_path : None or Path
        Getter and setter variable for project_path
    __bout_inp_src_dir : None or Path
        Getter and setter variable for bout_inp_src_dir
    __bout_inp_dst_dir : None or Path
        Getter and setter variable for bout_inp_dst_dir
    project_path : Path
        The root path of the project
    bout_inp_src_dir : Path
        The path to the BOUT.inp source directory
    bout_inp_dst_dir : Path
        The path to the BOUT.inp destination directory

    Methods
    -------
    _copy_inp()
        Copy BOUT.inp from bout_inp_src_dir to bout_inp_dst_dir

    Examples
    --------
    >>> bout_paths = BoutPaths()
    >>> bout_paths.project_path
    PosixPath(/root/BOUT-dev/examples/conduction)

    >>> bout_paths.bout_inp_src_dir
    PosixPath(/root/BOUT-dev/examples/conduction/data)

    >>> bout_paths.bout_inp_dst_dir
    PosixPath(/root/BOUT-dev/examples/conduction/2020-02-12_21-59-00_227295)

    >>> bout_paths.bout_inp_dst_dir = 'foo'
    PosixPath(/root/BOUT-dev/examples/conduction/foo)
    """

    def __init__(self,
                 project_path=None,
                 bout_inp_src_dir=None,
                 bout_inp_dst_dir=None):
        """
        Set the paths.

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
            The path to the BOUT.inp bout_inp_dst_dir directory
            (relative to self.project_path)
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
        Set the properties of self.project_path

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
        Set the properties of bout_inp_src_dir.

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

        Raises
        ------
        FileNotFoundError
            If no BOUT.inp file is found in the directory
        """
        return self.__bout_inp_src_dir

    @bout_inp_src_dir.setter
    def bout_inp_src_dir(self, bout_inp_src_dir):
        bout_inp_src_dir = \
            Path(bout_inp_src_dir) if bout_inp_src_dir is not None\
            else Path('data')

        self.__bout_inp_src_dir = \
            self.project_path.joinpath(bout_inp_src_dir)

        if not self.__bout_inp_src_dir.joinpath('BOUT.inp').is_file():
            raise FileNotFoundError(f'No BOUT.inp file found in '
                                    f'{self.__bout_inp_src_dir}')

        logging.debug('self.bout_inp_src_dir set to %s',
                      self.__bout_inp_src_dir)

        # Copy file to bout_inp_dst_dir if set
        if self.bout_inp_dst_dir is not None:
            self._copy_inp()

    @property
    def bout_inp_dst_dir(self):
        """
        Set the properties of bout_inp_dst_dir.

        The setter will convert bout_inp_dst_dir an absoulte path
        (as the input is relative to the project path), and copy
        BOUT.inp from self.bout_inp_src_dir to self.bout_inp_dst_dir

        Parameters
        ----------
        bout_inp_dst_dir : None or str or Path
            The path to the BOUT.inp bout_inp_dst_dir directory
            (relative to self.project_path)
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
        """Copy BOUT.inp from bout_inp_src_dir to bout_inp_dst_dir."""
        if self.bout_inp_src_dir != self.bout_inp_dst_dir:
            src = self.bout_inp_src_dir.joinpath('BOUT.inp')
            dst = self.bout_inp_dst_dir.joinpath(src.name)
            shutil.copy(src, dst)
            logging.debug('Copied %s to %s', src, dst)


class RunParameters:
    """
    Class which sets run parameters with precedence over BOUT.inp

    Attributes
    ----------
    __run_parameters_dict : None or dict
        Getter and setter variable for run_parameters_dict
    __run_parameters_str : None or str
        Getter and setter variable for run_parameters_str
    run_parameters_dict : dict
        The run parameters on dictionary form
   run_parameters_str : None or str
        The run parameters on string form

    Examples
    --------
    >>> run_parameters = RunParameters({'mesh':  {'nx': 4}})
    >>> run_parameters.run_parameters_str
    'mesh.nx=4 '

    >>> run_parameters.run_parameters_str = 'foo'
    Traceback (most recent call last):
      File "<input>", line 1, in <module>
    AttributeError: The run_parameters_str is read only, and is set
    internally in the setter of run_parameters_dict
    """
    def __init__(self, run_parameters_dict=None):
        """
        Set the parameters.

        Parameters
        ----------
        run_parameters_dict : None or dict of str, dict
            Options on the form
            >>> {'global': {'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}

        Notes
        -----
        The parameters set here will override those found in the
        BOUT.inp file
        """
        # Declare variables to be used in the getters and setters
        self.__run_parameters_dict = None
        self.__run_parameters_str = None

        # NOTE: run_parameters_srt will be set in the setter of
        #       run_parameters_dict
        self.run_parameters_srt = None

        # Set the parameters dict (and create the parameters string)
        self.run_parameters_dict = run_parameters_dict

    @property
    def run_parameters_dict(self):
        """
        Set the properties of self.run_parameters_dict

        The setter will also create the self.__run_parameters_str

        Parameters
        ----------
        run_parameters_dict : None or dict of str, dict
            Options on the form
            >>> {'global': {'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}

        Returns
        -------
        self.__run_parameters_dict : dict of str, dict
            Absolute path to the root of make file
        """
        return self.__run_parameters_dict

    @run_parameters_dict.setter
    def run_parameters_dict(self, run_parameters_dict):
        # Set the run parameters
        self.__run_parameters_dict = run_parameters_dict if \
            run_parameters_dict is not None else dict()

        # Generate the string
        sections = list(self.run_parameters_dict.keys())
        self.__run_parameters_str = ''
        if 'global' in sections:
            # Removing global, as this is something added in
            # obtain_project_parameters
            sections.remove('global')
            global_option = self.run_parameters_dict['global']
            for key, val in global_option.items():
                self.__run_parameters_str += f'{key}={val} '

        for section in sections:
            for key, val in run_parameters_dict[section].items():
                self.__run_parameters_str += f'{section}.{key}={val} '
        logging.debug('Parameters set to %s',
                      self.__run_parameters_str)

    @property
    def run_parameters_str(self):
        """
        Set the properties of self.run_parameters_str

        Returns
        -------
        self.__parameters_str : str
            The parameters dict serialized as a string

        Notes
        -----
        As the run_parameters_str must reflect run_parameters_dict,
        both are set when setting run_parameters_dict
        """
        return self.__run_parameters_str

    @run_parameters_str.setter
    def run_parameters_str(self, _):
        msg = (f'The run_parameters_str is read only, and is '
               f'set in run_parameters_dict (currently in use:'
               f'{self.run_parameters_str})')
        raise AttributeError(msg)


class ProcessorSplit:
    """
    Class which sets the processor split

    Attributes
    ----------
    __number_of_processors : None or int
        Getter and setter variable for number_of_processors
    __number_of_nodes : None or int
        Getter and setter variable for number_of_nodes
    __processors_per_node : None or int
        Getter and setter variable for processors_per_node
    number_of_processors : int
        The total number of processors to use
    number_of_nodes : int
        How many nodes to run on (only effective on clusters)
    processors_per_node : int
        The number of processors to allocate per node
        (only effective on clusters)

    Examples
    --------
    >>> processor_split = ProcessorSplit(number_of_processors=1,
    ...                                  number_of_nodes=1,
    ...                                  processors_per_node=1)

    >>> processor_split = ProcessorSplit(number_of_processors=2,
    ...                                  number_of_nodes=1,
    ...                                  processors_per_node=1)
    Traceback (most recent call last):
      File "<input>", line 1, in <module>
    ValueError: number_of_nodes*processors_per_node = 1, whereas
    number_of_processors = 2
    """

    def __init__(self,
                 number_of_processors=1,
                 number_of_nodes=1,
                 processors_per_node=1):
        """
        Set the parameters.

        Parameters
        ----------
        number_of_processors : int
            The total number of processors to use
        number_of_nodes : int
            How many nodes to run on (only effective on clusters)
        processors_per_node : int
            The number of processors to allocate per node
            (only effective on clusters)
        """
        # Declare variables to be used in the getters and setters
        self.__number_of_processors = None
        self.__number_of_nodes = None
        self.__processors_per_node = None

        # Set the number of processors
        self.number_of_processors = number_of_processors

        # Set the number of nodes
        self.number_of_nodes = number_of_nodes

        # Set the processors per node
        self.processors_per_node = processors_per_node

    @property
    def number_of_processors(self):
        """
        Set the properties of self.number_of_processors

        Parameters
        ----------
        number_of_processors : int
            The number of processors

        Returns
        -------
        self.__number_of_processors : int
            The number of processors
        """
        return self.__number_of_processors

    @number_of_processors.setter
    def number_of_processors(self, number_of_processors):
        self.__number_of_processors = number_of_processors
        if self.number_of_nodes is not None \
                and self.processors_per_node is not None:
            self.__enough_nodes_check()

        logging.debug('number_of_processors set to %s',
                      number_of_processors)

    @property
    def number_of_nodes(self):
        """
        Set the properties of self.number_of_nodes

        Parameters
        ----------
        number_of_nodes : int
            The number of processors

        Returns
        -------
        self.__number_of_nodes : int
            The number of nodes
        """
        return self.__number_of_nodes

    @number_of_nodes.setter
    def number_of_nodes(self, number_of_nodes):
        self.__number_of_nodes = number_of_nodes
        if self.number_of_processors is not None \
                and self.processors_per_node is not None:
            self.__enough_nodes_check()
        logging.debug('number_of_nodes set to %s',
                      number_of_nodes)

    @property
    def processors_per_node(self):
        """
        Set the properties of self.processors_per_node

        Parameters
        ----------
        processors_per_node : int
            The number of processors

        Returns
        -------
        self.__processors_per_node : int
            The number of nodes
        """
        return self.__processors_per_node

    @processors_per_node.setter
    def processors_per_node(self, processors_per_node):
        self.__processors_per_node = processors_per_node
        if self.number_of_processors is not None \
                and self.number_of_nodes is not None:
            self.__enough_nodes_check()
        logging.debug('processors_per_node set to %s',
                      processors_per_node)

    def __enough_nodes_check(self):
        """Check that enough nodes are allocated."""
        product = self.number_of_nodes*self.processors_per_node
        if product < self.number_of_processors:
            msg = (f'number_of_nodes*processors_per_node = {product}, '
                   f'whereas number_of_processors = '
                   f'{self.number_of_processors}')
            raise ValueError(msg)


class BoutRunner:
    """
    The basic runner class.

    Examples
    --------
    FIXME
    >>> from bout_runners.runners.base_runner import BoutRunner
    >>> runner = BoutRunner('path/to/project/root')
    >>> runner.run()
    """

    def __init__(self,
                 bout_paths,
                 run_parameters=RunParameters(),
                 processor_split=ProcessorSplit(),
                 database_root_path=None):
        """
        Set the input parameters and create the database.

        Parameters
        ----------
        bout_paths : BoutPaths
            Class which contains the paths
        run_parameters : RunParameters
            Class containing the run parameters
        processor_split : ProcessorSplit
            Class containing the processor split
        database_root_path : None or Path or str
            Root path of the database file
            If None, the path will be set to $HOME/BOUT_db
        """
        # Set member data
        self.bout_paths = bout_paths
        self.run_parameters = run_parameters
        self.processor_split = processor_split

        # Get database
        db_path = get_db_path(project_path=self.bout_paths.project_path,
                              database_root_path=database_root_path)
        self.bookkeeper = Bookkeeper(db_path)

        self.make = None  # Set to make-obj in self.make_project

    def make_project(self):
        """Set the make object and Make the project."""
        self.make = MakeProject(self.bout_paths.project_path)
        self.make.run_make()

    def run(self):
        """Execute a BOUT++ run."""
        # Make the project if not already made
        self.make_project()

        mpi_cmd = 'mpirun -np'

        # NOTE: No spaces if parameters are None
        command = (f'{mpi_cmd} '
                   f'{self.processor_split.number_of_processors} '
                   f'./{self.make.exec_name} '
                   f'-d {self.bout_paths.bout_inp_dst_dir} '
                   f'{self.run_parameters.run_parameters_str}')

        db_ready = tables_created(self.bookkeeper)
        if db_ready:
            self.bookkeeper.store_data_from_run(
                self,
                self.processor_split.number_of_processors)
        else:
            logging.warning('Database %s has no entries and is not '
                            'ready. '
                            'No data capture will be made.',
                            self.bookkeeper.database_path)

        run_subprocess(command, path=self.bout_paths.project_path)
        if db_ready:
            self.bookkeeper.update_status()
