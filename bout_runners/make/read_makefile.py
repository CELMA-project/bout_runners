"""Module containing the classes to read the makefile."""


import re
from pathlib import Path


class ReadMakefileError(Exception):
    """Error class indicating that this is a ReadMakefile error."""

    def __init__(self, variable, path):
        """
        Construct a string and call the super constructor.

        Parameters
        ----------
        variable : str
            Variable to searched for
        path : Path or str
            Path searched at
        """
        message = f'Could not find {variable} in {path}'

        super().__init__(message)


class ReadBoutMakefile:
    """
    Class which reads a BOUT++ Makefile.

    Attributes
    ----------
    path : Path
        The path to the Makefile
    content : str
        The content of the Makefile as a string
    """

    def __init__(self, path):
        """
        Read the content of a Makefile and store it into self.content.

        Parameters
        ----------
        path : Path or str
            Path to the Makefile
        """
        self.path = path
        self.content = self.read()

    def read(self):
        """
        Read the makefile.

        Returns
        -------
        str
            Content of the Makefile
        """
        with Path(self.path).open('r') as make_file:
            return make_file.read()

    def get_variable_value(self):
        """Get the value of the variable."""
        pass


class BoutMakefileVariable(ReadBoutMakefile):
    """
    Class which reads a variable from a BOUT++ Makefile.

    Attributes
    ----------
    variable_name : str
        Name of the variable belonging to the instance
    variable_value : str
        Value belonging to the variable of the instance

    Methods
    -------
    get_variable_value()
        Get the value of the variable

    Examples
    --------
    Makefile
    >>> BOUT_SUPER	= /super/path/to/BOUT-dev
    ... BOUT_TOP	= $(BOUT_SUPER)/BOUT-dev
    ...
    ... SOURCEC		= bout_model.cxx
    ...
    ... include $(BOUT_TOP)/make.config

    Script
    >>> BoutMakefileVariable('SOURCEC', 'Makefile').get_variable_value()
    'bout_model.cxx'
    """

    def __init__(self, path, variable_name):
        """
        Set the variable name of the instance.

        Parameters
        ----------
        path : Path or str
            The path to the Makefile
        variable_name : str
            The variable under consideration
        """
        super(BoutMakefileVariable, self).__init__(path)

        self.variable_name = variable_name
        self.variable_value = None

    def get_variable_value(self):
        """
        Get the value of the variable.

        Returns
        -------
        self.variable_value : str
            The last match of the variable

        Raises
        ------
        ReadMakefileError
            If self.variable is not found

        Examples
        --------
        Makefile
        >>> # foo=bar
        ... foo = baz
        ... foo   = foobar.qux # foo = quux.quuz

        Script
        >>> BoutMakefileVariable('foo', 'Makefile').get_variable_value()
        'foobar.qux'
        """
        # Build the match function for the regex
        no_comment_line = r'^\s*(?!#)'
        must_contain_eq_sign = r'\s*=\s*'
        # As we are using the MULTILINE modifier we should exclude
        # newlines
        capture_all_except_comment_and_newline = r'([.]*[^#\n]*)'
        avoid_trailing_whitespace = r'(?<!\s)'

        pattern = f'{no_comment_line}{self.variable_name}' \
                  f'{must_contain_eq_sign}' \
                  f'{capture_all_except_comment_and_newline}' \
                  f'{avoid_trailing_whitespace}'

        # NOTE: match() checks for match only at the beginning of a
        #       string
        #       search() checks for match anywhere in the string,
        #       but returns only the first occurrence
        #       findall() matches all matching group as a list,
        #       and as we only have one matching group in the
        #       pattern the elements in the list will be string not a
        #       tuple
        matches = re.findall(pattern, self.content, re.M)

        if len(matches) == 0:
            raise ReadMakefileError(self.variable_name, self.path)

        # Only the last line of the variable will be considered by
        # the Makefile
        self.variable_value = matches[-1]

        return self.variable_value
