"""Contains the class dealing with the run parameters."""


import logging
from typing import Dict, Optional, Union


class RunParameters:
    """
    Class which deals with run parameters with precedence over BOUT.inp.

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

    def __init__(
        self,
        run_parameters_dict: Optional[
            Dict[str, Dict[str, Union[int, bool, str]]]
        ] = None,
    ) -> None:
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

        # Set the parameters dict (and create the parameters string)
        self.run_parameters_dict = run_parameters_dict

    @property
    def run_parameters_dict(self):
        """
        Set the properties of self.run_parameters_dict.

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
        self.__run_parameters_dict = (
            run_parameters_dict if run_parameters_dict is not None else dict()
        )

        # Generate the string
        sections = list(self.run_parameters_dict.keys())
        self.__run_parameters_str = ""
        if "global" in sections:
            # Removing global, as this is something added in
            # obtain_project_parameters
            sections.remove("global")
            global_option = self.run_parameters_dict["global"]
            for key, val in global_option.items():
                self.__run_parameters_str += f"{key}={val} "

        for section in sections:
            for key, val in run_parameters_dict[section].items():
                self.__run_parameters_str += f"{section}.{key}={val} "
        logging.debug("Parameters set to %s", self.__run_parameters_str)

    @property
    def run_parameters_str(self):
        """
        Set the properties of self.run_parameters_str.

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
        msg = (
            f"The run_parameters_str is read only, and is "
            f"set in run_parameters_dict (currently in use: "
            f"{self.run_parameters_str})"
        )
        raise AttributeError(msg)
