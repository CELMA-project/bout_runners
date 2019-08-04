import inspect
import logging
from pathlib import Path
from bout_runners.utils.subprocesses_functions import run_subprocess
logger = logging.getLogger(__name__)


class MakeError(Exception):
    """
    Error class indicating that this is a Make error
    """
    pass


class MakeProject(object):
    """
    The make class is responsible for making the project

    Attributes
    ----------

    Methods
    -------

    Examples
    --------

    """

    def __init__(self,
                 makefile_root_path=None,
                 force=False,
                 makefile_name=None):
        """
        Calls the make file of the makefile_root_path

        Parameters
        ----------
        makefile_root_path : None or Path or str
            Root path of make file
            If None, the path of the root caller of MakeProject will
            be called
        force : bool
            Will make the make file if True, even if an executable
            exists
        makefile_name : None or str
            If set to None, it tries the following names, in order:
            'GNUmakefile', 'makefile' and 'Makefile'
        """

        if makefile_root_path is None:
            # Assume that the makefile_root_path is located in the
            # caller directory
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            makefile_root_path = Path(module.__file__).parent

        self.makefile_root_path = Path(makefile_root_path)

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
        run_subprocess(command, self.makefile_root_path)
        logger.info("Making the .cxx program")
        command = "make"
        run_subprocess(command, self.makefile_root_path)
