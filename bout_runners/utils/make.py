import inspect
import logging
from pathlib import Path
from bout_runners.utils.subprocesses_functions import run_subprocess
from bout_runners.utils.exec_name import get_exec_name


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
                 makefile_name=None):
        """
        Calls the make file of the makefile_root_path

        Parameters
        ----------
        makefile_root_path : None or Path or str
            Root path of make file
            If None, the path of the root caller of MakeProject will
            be called
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
        self.makefile_name = makefile_name

    def run_make(self, force=False):
        """
        Runs make in the self.makefile_root_path

        If an executable is found, nothing will be done unless 'force'
        is set to True

        Parameters
        ----------
        force : bool
            If True, make clean will be called prior to make
        """

        # Check if already made
        exec_name = get_exec_name(self.makefile_root_path,
                                  self.makefile_name)
        if force:
            self.run_clean()

        made = self.makefile_root_path.joinpath(exec_name).is_file()

        if not made:
            make_str = 'make' if self.makefile_name is None \
                else f'make -f {self.makefile_name}'

            logger.info('Making the program')
            command = f'{make_str}'
            run_subprocess(command, self.makefile_root_path)

    def run_clean(self):
        """
        Runs make clean in the self.makefile_root_path
        """
        make_str = 'make' if self.makefile_name is None \
            else f'make -f {self.makefile_name}'

        logger.info('Running make clean')
        command = f'{make_str} clean'
        run_subprocess(command, self.makefile_root_path)
