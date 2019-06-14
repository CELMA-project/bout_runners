"""
Contains the make class
"""

import inspect
import logging
from pathlib import Path
from bout_runners.utils.subprocesses_functions import run_subprocess
logger = logging.getLogger(__name__)


class MakeProject(object):
    """
    The make class is responsible for making the project

    Attributes
    ----------

    Examples
    --------

    """

    def __init__(self, make_file_root_path=None, name=None):
        """
        Constructor for MakeProject

        Parameters
        ----------
        make_file_root_path : None or Path or str
            Root path of make file
            If None, the path of the root caller of MakeProject will
            be called
        name : None or str

        """

        if make_file_root_path is None:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            make_file_root_path = module.__file__

        make_file_root_path = Path(make_file_root_path)
        print(make_file_root_path)


if __name__ == '__main__':
    MakeProject()


def run_make():
    """
    Make cleans and makes the .cxx program
    """

    logger.info("Make clean previously compiled code")
    command = "make clean"
    run_subprocess(command, path)
    logger.info("Making the .cxx program")
    command = "make"
    run_subprocess(command, path)


def set_program_name(self, prog_name=None):
    """
    Will set self._program_name and make the program if the
    prog_name.o file is not found.

    Parameters
    ----------
    prog_name : str
        Name of the exceutable. If None, the name will be set from
        the *.o file.
    """

    if prog_name is not(None):
        # Check that a string is given
        if not isinstance(prog_name, str):
            message = "prog_name must be given as a string"
            self._errors.append("TypeError")
            raise TypeError(message)
        # Search for file
        if os.path.isfile(prog_name):
            self._program_name = prog_name
        else:
            print("{} not found, now making:".format(prog_name))
            # File not found, make
            self._run_make()
            # Set the make flag to False, so it is not made again
            self._make = False
            # Search for file
            if not(os.path.isfile(prog_name)):
                message = ("{} could not be found after make. "
                           "Please check for spelling mistakes").\
                    format(prog_name)
                self._errors.append("RuntimeError")
                raise RuntimeError(message)
            else:
                self._program_name = prog_name
    else:
        # Find the *.o file
        o_files = glob.glob("*.o")
        if len(o_files) > 1:
            message = ("More than one *.o file found. "
                       "The first *.o file is chosen. "
                       "Consider setting 'prog_name'.")
            self._warning_printer(message)
            self._warnings.append(message)
            self._program_name = o_files[0].replace(".o", "")
        elif len(o_files) == 1:
            # Pick the first instance as the name
            self._program_name = o_files[0].replace(".o", "")
        else:
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
            else:
                self._errors.append("RuntimeError")
                raise RuntimeError(
                    "No make file found in current directory")