"""Contains the class dealing with the final parameters."""


import ast
from bout_runners.parameters.run_parameters import RunParameters


class FinalParameters:
    r"""
    Class which deals with the final parameters.

    The final parameters are those who are going to be used in the
    execution of the project

    Attributes
    ----------
    self.__default_parameters : DefaultParameters
        Object dealing with the default parameters
    self.__run_parameters = run_parameters
        Object dealing with the run parameters

    Methods
    -------
    get_final_parameters()
        Obtain the final parameters that will be used in a run
    cast_parameters_to_sql_type(parameter_dict)
        Cast the values of a parameter dict to valid SQL types

    Examples
    --------
    Import dependencies
    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.parameters.default_parameters import \
    ...     DefaultParameters
    >>> from bout_runners.parameters.run_parameters import \
    ...     RunParameters

    Create the `bout_paths` object
    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source',
    ... 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination',
    ... 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Get the final parameters
    >>> default_parameters = DefaultParameters(bout_paths=bout_paths)
    >>> final_parameters = FinalParameters(default_parameters,
    ...     RunParameters({'nout': 10}))
    >>> final_parameters_dict = final_parameters.get_final_parameters()
    >>> final_parameters_dict
    {'global': {'append': False, 'async_send': False, ..., 'nout':
    10, ...}}

    >>> final_parameters.\
    ...     cast_parameters_to_sql_type(final_parameters_dict)
    {'global': {'append': TEXT, 'async_send': TETX, ..., 'nout':
    INTEGER, ...}}
    """

    def __init__(self,
                 default_parameters,
                 run_parameters=RunParameters()):
        """
        Set the member data.

        Parameters
        ----------
        default_parameters : DefaultParameters
            Object dealing with default parameters (i.e. standard
            BOUT++ parameters, or those given in BOUT.inp)
        run_parameters : RunParameters
            Object dealing with run parameters (i.e. parameters set
            in bout_runner which has precedence over BOUT.inp)
        """
        self.__default_parameters = default_parameters
        self.__run_parameters = run_parameters

    def get_final_parameters(self):
        """
        Obtain the final parameters that will be used in a run.

        Returns
        -------
        final_parameters_dict : dict of str, dict
            Parameters on the form
            >>> {'global':{'append': 'False', 'nout': 5},
            ...  'mesh':  {'nx': 4},
            ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}
        """
        final_parameters_dict = \
            self.__default_parameters.get_default_parameters()
        run_parameters_dict = self.__run_parameters.run_parameters_dict
        final_parameters_dict.update(run_parameters_dict)

        # Cast True to 1 and False to 0 as SQLite has no support for
        # bool
        sections = final_parameters_dict.keys()
        for section in sections:
            parameters = final_parameters_dict[section].keys()
            for parameter in parameters:
                val = final_parameters_dict[section][parameter]
                if isinstance(val, bool):
                    if val:
                        final_parameters_dict[section][parameter] = 1
                    else:
                        final_parameters_dict[section][parameter] = 0

        return final_parameters_dict

    @staticmethod
    def cast_parameters_to_sql_type(parameter_dict):
        """
        Cast the values of a parameter dict to valid SQL types.

        Parameters
        ----------
        parameter_dict : dict
            Dictionary containing the parameters given in BOUT.settings
            On the form
            >>> {'section': {'parameter': 'value'}}

        Returns
        -------
        parameter_dict_as_sql_types : dict
            Dictionary containing the parameters given in BOUT.settings
            On the form
            >>> {'section': {'parameter': 'value_type'}}
        """
        type_map = {'bool': 'INTEGER',  # No bool type in SQLite
                    'float': 'REAL',
                    'int': 'INTEGER',
                    'str': 'TEXT'}

        parameter_dict_as_sql_types = parameter_dict.copy()

        for section in parameter_dict.keys():
            for key, val in parameter_dict[section].items():
                # If type is not found, type is str
                try:
                    val_type = type(ast.literal_eval(val))
                except (SyntaxError, ValueError):
                    val_type = str

                parameter_dict[section][key] = type_map[
                    val_type.__name__]

        return parameter_dict_as_sql_types
