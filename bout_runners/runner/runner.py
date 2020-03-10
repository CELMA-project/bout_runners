"""Contains the BOUT runner class."""


import re
import logging
import ast
import configparser
from pathlib import Path
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.bookkeeper.bookkeeper import Bookkeeper
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.run_parameters import RunParameters
from bout_runners.executor.executor import Executor
from bout_runners.submitter.local_submitter import LocalSubmitter


class BoutRunner:
    """
    Executes the command for submitting a run.

    FIXME: Add variables and attributes

    FIXME: Add examples
    """

    def __init__(self,
                 executor,
                 database_connector):
        """
        FIXME

        Parameters
        ----------
        FIXME
        """
        # Set member data
        self.__executor = executor
        self.__database_creator = DatabaseCreator(database_connector)
        self.__bookkeeper = Bookkeeper(database_connector)

    def create_schema(self):
        """
        Create the schema.

        The schema is created by executing a settings run in order to
        infer the parameters of the project executable. The
        parameters are subsequently read and their types cast to
        SQL types
        """
        settings_path = self.run_settings_run()
        parameter_dict = self.obtain_project_parameters(settings_path)
        parameter_dict_as_sql_types = \
            self.cast_parameters_to_sql_type(parameter_dict)
        self.__database_creator.create_all_schema_tables(
            parameter_dict_as_sql_types)

    @staticmethod
    def obtain_project_parameters(settings_path):
        """
        Return the project parameters from the settings file.

        Parameters
        ----------
        settings_path : Path
            Path to the settings file

        Returns
        -------
        run_parameters_dict : dict
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
        """
        # The settings file lacks a header for the global parameter
        # Therefore, we add add the header [global]
        with settings_path.open('r') as settings_file:
            settings_memory = f'[global]\n{settings_file.read()}'

        config = configparser.ConfigParser()
        config.read_string(settings_memory)

        parameter_dict = dict()

        for section in config.sections():
            parameter_dict[section] = dict()
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
                    val = stripped_val

                parameter_dict[section][key] = val

        # NOTE: Bug in .settings: -d path is captured with # not in use
        bout_inp_dir = settings_path.parent
        parameter_dict['global'].pop('d', None)
        parameter_dict['global'].pop(str(bout_inp_dir).lower(), None)

        if 'all' in parameter_dict.keys():
            parameter_dict['all_boundaries'] = parameter_dict.pop('all')

        # Drop run as bout_runners will make its own table with that
        # name
        parameter_dict.pop('run', None)

        return parameter_dict

    @staticmethod
    def cast_parameters_to_sql_type(parameter_dict):
        """
        Return the project parameters from the settings file.

        Parameters
        ----------
        parameter_dict : dict
            Dictionary containing the parameters given in BOUT.settings
            On the form
            >>> {'section': {'parameter': 'value'}}

        Returns
        -------
        parameter_dict_sql_types : dict
            Dictionary containing the parameters given in BOUT.settings
            On the form
            >>> {'section': {'parameter': 'value_type'}}
        """
        type_map = {'bool': 'INTEGER',  # No bool type in SQLite
                    'float': 'REAL',
                    'int': 'INTEGER',
                    'str': 'TEXT'}

        parameter_dict_sql_types = parameter_dict.copy()

        for section in parameter_dict.keys():
            for key, val in parameter_dict[section].items():
                # If type is not found, type is str
                try:
                    val_type = type(ast.literal_eval(val))
                except (SyntaxError, ValueError):
                    val_type = str

                parameter_dict[section][key] = type_map[
                    val_type.__name__]

        return parameter_dict_sql_types

    def run_settings_run(self):
        """
        Perform a settings run.

        A settings run executes the executable of the project with
        nout = 0 in order to capture all parameters used in the project

        Returns
        -------
        settings_path : Path
            Path to the settings file
        """
        project_path = self.__executor.bout_paths.project_path

        bout_paths = BoutPaths(
            project_path=project_path,
            bout_inp_src_dir=
            self.__executor.bout_paths.bout_inp_src_dir,
            bout_inp_dst_dir='settings_run')
        run_parameters = RunParameters({'global': {'nout': 0}})
        executor = Executor(
            bout_paths=bout_paths,
            submitter=LocalSubmitter(project_path),
            run_parameters=run_parameters)

        settings_path = \
            bout_paths.bout_inp_dst_dir.joinpath('BOUT.settings')

        if not settings_path.is_file():
            logging.info('Running settings run as the parameters of '
                         'the project are unknown')
            executor.execute()

        return settings_path

    def run(self):
        """Perform the run and capture data."""
        if not self.__bookkeeper.database_reader.check_tables_created():
            logging.info('Creating schema as no tables were found in '
                         '%s',
                         self.__bookkeeper.database_reader.
                         database_connector.database_path)
            self.create_schema()

        self.__bookkeeper.capture_new_data_from_run(
            self,
            self.__executor.processor_split)

        self.__bookkeeper.database_reader.update_status()
