"""
Contains the make class
"""

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
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            make_file_root_path = Path(module.__file__).parent

        self.make_file_root_path = Path(make_file_root_path)

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
            If no *.o file is found
        """

        o_files = self.make_file_root_path.glob("*.o")

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
            self._errors.append("RuntimeError")
            raise RuntimeError(
                "No make file found in current directory")


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
