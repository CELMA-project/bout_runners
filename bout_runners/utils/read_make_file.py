import re
from pathlib import Path


class ReadMakeFileError(Exception):
    """
    Error class indicating that this is a ReadMakeFile error
    """

    def __init__(self, variable, path):
        """
        Constructs a string and calls the super constructor

        Parameters
        ----------
        variable : str
            Variable to searched for
        path : Path or str
            Path searched at
        """
        message = f'Could not find {variable} in {path}'

        super().__init__(message)


class ReadBoutMakeFile(object):
    """
    Class which reads a BOUT++ MakeFile
    """

    def __init__(self, path):
        """
        Reads the content of a MakeFile and stores it into self.content

        Parameters
        ----------
        path : Path or str
            Path to the Makefile
        """

        self.path = path

        with Path(path).open('r') as f:
            self.content = f.read()


class BoutMakeFileVariable(ReadBoutMakeFile):
    """
    Class which reads a variable from a BOUT++ MakeFile
    """

    def __init__(self, path, variable_name):
        """
        Sets the variable name of the instance

        Parameters
        ----------
        path : Path or str
            The path to the MakeFile
        variable_name : str
            The variable under consideration
        """

        super(BoutMakeFileVariable, self).__init__(path)

        self.variable_name = variable_name
        self.variable_value = None

    def get_variable_value(self):
        """
        Get the value of the variable

        Returns
        -------
        self.variable_value : str
            The last match of the variable

        Raises
        ------
        ReadMakeFileError
            If self.variable is not found

        Examples
        --------
        MakeFile
        >>> # foo=bar
        ... foo = baz
        ... foo   = foobar.qux # foo = quux.quuz

        Script
        >>> BoutMakeFileVariable('foo', 'Makefile').get_variable_value()
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
            raise ReadMakeFileError(self.variable_name, self.path)

        # Only the last line of the variable will be considered by
        # the Makefile
        self.variable_value = matches[-1]

        return self.variable_value
