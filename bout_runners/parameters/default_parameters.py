"""Contains the class dealing with the default parameters."""


import re
import ast
import logging
import configparser
from pathlib import Path
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.executor.executor import Executor
from bout_runners.submitter.local_submitter import LocalSubmitter


class DefaultParameters:
    """
    Class which deals with the default parameters.

    The default parameters are those set internally in BOUT++,
    or in the specified BOUT.inp file

    Attributes
    ----------
    self.__bout_paths : BoutPaths
        Object for the BOUT++ paths
    self.__settings_path : None or Path
        Path to the BOUT.settings file

    Methods
    -------
    run_parameters_run()
        Execute a run to obtain the default parameters.
    get_default_parameters()
        Return the default parameters from the settings file.

    Examples
    --------
    The easiest way to use DefaultParameters is to run a script from the
    root directory of the project (i.e. where the `Makefile` and
    `data` directory are normally situated. The script can simply call
    >>> DefaultParameters().get_default_parameters()
    {'global': {'append': False, 'async_send': False, ...}}

    A more elaborate example where all the dependency objects are
    built manually:

    Import dependencies
    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths

    Create the `bout_paths` object
    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source',
    ... 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination',
    ... 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Get the default parameters
    >>> default_parameter = DefaultParameters(bout_paths=bout_paths)
    >>> default_parameter.get_default_parameters()
    {'global': {'append': False, 'async_send': False, ...}}
    """

    def __init__(self, bout_paths=None, settings_path=None):
        """
        Set the member data.

        If the settings_path is None, the constructor will call
        run_parameters_run to create a settings_path

        Warnings
        --------
        There can be a potential mismatch between a user provided
        `settings_path` and the actual default values. This can occur
        if the user has updated `BOUT.inp` without updating the
        `BOUT.settings` file. It is therefore recommended to set
        `settings_path` to None unless the user is sure the
        `BOUT.settings` file pointed to by `settings_path` is up to date

        Parameters
        ----------
        bout_paths : BoutPaths or None
            Object containing the paths of the project
            Will only be used in the `run_parameters_run` call if the
            `settings_path` is not valid
        settings_path : None or Path
            Path to the up-to-date `settings_path`
            Will invoke `run_parameters_run` if set to None
        """
        self.__bout_paths = bout_paths
        self.__settings_path = Path() if settings_path is None \
            else Path(settings_path)

        if not self.__settings_path.is_file():
            logging.info('Running parameter run as the parameters of '
                         'the project are unknown')
            self.run_parameters_run(self.__bout_paths)

    def run_parameters_run(self, bout_paths):
        """
        Execute a run to obtain the default parameters.

        A settings run executes the executable of the project with
        nout = 0 in order to capture all parameters used in the project

        Parameters
        ----------
        bout_paths : BoutPaths
            Object containing the paths of the project
        """
        if bout_paths is None:
            bout_paths = BoutPaths(bout_inp_dst_dir='settings_run')
        else:
            bout_paths.bout_inp_dst_dir = 'settings_run'

        run_parameters = RunParameters({'global': {'nout': 0}})
        executor = Executor(
            bout_paths=bout_paths,
            submitter=LocalSubmitter(bout_paths.project_path),
            run_parameters=run_parameters)

        executor.execute()

        self.__settings_path = \
            bout_paths.bout_inp_dst_dir.joinpath('BOUT.settings')

    def get_default_parameters(self):
        """
        Return the default parameters from the settings file.

        Returns
        -------
        default_parameters_dict : dict
            Dictionary containing the parameters given in BOUT.settings
            On the form
            >>> {'section': {'parameter': 'value'}}

        Notes
        -----
        1. The section-less part of BOUT.settings will be renamed
           `global`
        2. In the `global` section, the keys `d` and the directory to
           the BOUT.inp file will be removed
        3. If the section `all` is present in BOUT.settings, the section
           will be renamed `all_boundaries` as `all` is a protected SQL
           keyword
        4. The section `run` will be dropped due to bout_runners own
           `run` table
        5. The string values will be stored using lowercase
        """
        # The settings file lacks a header for the global parameter
        # Therefore, we add add the header [global]
        with self.__settings_path.open('r') as settings_file:
            settings_memory = f'[global]\n{settings_file.read()}'

        config = configparser.ConfigParser()
        config.read_string(settings_memory)

        default_parameters_dict = dict()

        for section in config.sections():
            default_parameters_dict[section] = dict()
            for key, val in config[section].items():
                # Strip comments
                capture_all_but_comment = '^([^#]*)'
                matches = re.findall(capture_all_but_comment, val, re.M)

                # Exclude comment line
                if len(matches) == 0:
                    continue

                # Capitalize in case of boolean
                stripped_val = matches[0].capitalize()

                # If type is not found, type is str
                try:
                    val = ast.literal_eval(stripped_val)
                except (SyntaxError, ValueError):
                    # Strip the whitespaces and cast to lowercase
                    val = stripped_val.strip().lower()

                default_parameters_dict[section][key] = val

        # NOTE: Bug in .settings: -d path is captured with # not in use
        bout_inp_dir = self.__settings_path.parent
        default_parameters_dict['global'].pop('d', None)
        default_parameters_dict['global'].pop(str(bout_inp_dir).lower(),
                                              None)

        if 'all' in default_parameters_dict.keys():
            default_parameters_dict['all_boundaries'] = \
                default_parameters_dict.pop('all')

        # Drop run as bout_runners will make its own table with that
        # name
        default_parameters_dict.pop('run', None)

        return default_parameters_dict
