"""
Contains the make class
"""

import re
import inspect
import logging
from pathlib import Path
from bout_runners.utils.subprocesses_functions import run_subprocess
logger = logging.getLogger(__name__)


# FIXME: We might need the exec_name elsewhere, as the executable is
#        needed for a run


class MakeError(Exception):
    """
    Error class indicating that this is a Make error
    """
    pass


class ReadMakeFileError(Exception):
    """
    Error class indicating that this is a ReadMakeFile error
    """

    def __init__(self, variable, path):
        """
        Constructs a string and calls the super constructor

        Parameters
        ----------
        variable
        path
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
        matches = re.findall(pattern, self.content)

        if len(matches) == 0:
            raise ReadMakeFileError(self.variable_name, self.path)

        # Only the last line of the variable will be considered by
        # the Makefile
        self.variable_value = matches[-1]

        return self.variable_value

    def _get_variable_rhs_stem(self, rhs_to_find, variable_line):
        """
        Get the stem of the right hand side of a variable

        # FIXME: YOU ARE HERE: CHCEK REGEX101, MAKE EXAMPLE HERE AND
        ABOVE

        Parameters
        ----------
        variable_to_find

        Returns
        -------
        str or None

        Examples
        --------
        >>> self._
        """

        rhs_stem = None

        # Build the match function for the regex
        must_containt_eq_sign = r'\s*=\s*'
        valid_stem_group = r'=\s*([^#\[\\/:*?\"<>|.\]]*)'

        pattern = f'{must_containt_eq_sign}{valid_stem_group}'

        match = re.search(pattern, variable_line)

        if not match is None:
            rhs_stem = match.group(1)

        return rhs_stem


class MakeProject(object):
    """
    The make class is responsible for making the project

    Methods
    -------

    Examples
    --------

    """

    def __init__(self,
                 make_file_root_path=None,
                 exec_name=None,
                 force=False):
        """
        Calls the make file of the make_file_root_path

        Parameters
        ----------
        make_file_root_path : None or Path or str
            Root path of make file
            If None, the path of the root caller of MakeProject will
            be called
        exec_name : None or str
            Name of the resulting executable.
            This is used to check if the program has been made
            If None the class will search for *.o file, and infer the
            name from the first *.o file found.
            The executable will not be made if a file is found,
            unless force is set to True
        force : bool
            Will make the make file if True, even if an executable
            exists
        """

        if make_file_root_path is None:
            # Assume that the make_file_root_path is located in the
            # caller directory
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            make_file_root_path = Path(module.__file__).parent

        self.make_file_root_path = Path(make_file_root_path)

        # FIXME: Don't have print statements
        if force:
            print('Force make')
        else:
            if exec_name is not None:
                exec_name = self._get_exec_name()
            if not self._exec_exist(exec_name):
                print('Did not find any possible executable name for '
                      'the project')

    def _run_make(self):
        """
        Make cleans and makes the .cxx program
        """

        logger.info("Make clean previously compiled code")
        command = "make clean"
        run_subprocess(command, self.make_file_root_path)
        logger.info("Making the .cxx program")
        command = "make"
        run_subprocess(command, self.make_file_root_path)

    def _get_exec_name(self):
        """
        Returns the name of the executable if a *.o file is found

        Returns
        -------
        exec_name : str
            Name of the executable

        Raises
        ------
        MakeError
            If no valid executable file name is found
        """


    def _exec_exist(self, exec_name):
        """
        Check if an executable already exist

        Parameters
        ----------
        exec_name : str
            Name of the executable

        Returns
        -------
        bool
            Whether the executable is found or not
        """

        # Check if there exists a make
        make_file = glob.glob("*make*")
        if len(make_file) > 0:
            # Run make
            self._run_make()
            # Set the make flag to False, so it is not made again
            self._make = False
            # Search for the .o file again
            o_files = glob.glob("*.o")
            if len(o_files) > 0:
                self._program_name = o_files[0].replace(".o", "")
            else:
                self._program_name = False
                message = ("The constructor could not make your"
                           " program")
                self._errors.append("RuntimeError")
                raise RuntimeError(message)
