"""FIXME."""


import ast
import logging


# FIXME: YOU ARE HERE: Cleaning up the mess
class BoutInpParameters:
    """
    FIXME
    """

    def __init__(self):
        """

        """

    def extract_parameters_in_use(project_path,
                                  bout_inp_dst_dir,
                                  run_parameters_dict):
        """
        Extract parameters that will be used in a run.

        Parameters
        ----------
        project_path : Path
            Root path of project (make file)
        bout_inp_dst_dir : Path
            Path to the directory of BOUT.inp currently in use
        run_parameters_dict : dict of str, dict
            Options on the form
            >>> {'global':{'append': False, 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}

        Returns
        -------
        parameters : dict of str, dict
            Parameters on the same form as `run_parameters_dict`
            (from obtain_project_parameters)
        """
        # Obtain the default parameters
        settings_path = project_path.joinpath('settings_run',
                                              'BOUT.settings')
        if not settings_path.is_file():
            logging.info('No setting files found, running run_settings_run')
            self.run_settings_run(project_path)
        parameters = obtain_project_parameters(settings_path)
        # Update with parameters from BOUT.inp
        bout_inp_path = bout_inp_dst_dir.joinpath('BOUT.inp')
        parameters.update(obtain_project_parameters(bout_inp_path))
        # Update with parameters from run_parameters_dict
        parameters.update(run_parameters_dict)

        return parameters

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
