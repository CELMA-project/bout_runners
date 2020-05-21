"""Module containing the MakeProject class."""


import logging
from pathlib import PosixPath, Path
from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.utils.names import get_exec_name
from bout_runners.utils.names import get_makefile_path
from bout_runners.submitter.local_submitter import LocalSubmitter
from typing import Optional


class MakeError(Exception):
    """Error class indicating that this is a Make error."""


class Make:
    """
    Class for making the project.

    Attributes
    ----------
    makefile_root_path : Path
        The path to the Makefile
    makefile_name : str
        The name of the Makefile
    makefile_path : Path
        Path to the makefile
    exec_name : str
        The name of the executable

    Methods
    -------
    run_make(force=False)
        Runs make in the self.makefile_root_path
    run_clean()
        Runs make clean in the self.makefile_root_path

    Examples
    --------
    >>> from bout_runners.make.make import Make
    ... from pathlib import Path
    ... path = Path('path', 'to', 'makefile_root_path')
    ... make_obj = Make(makefile_root_path=path)
    ... make_obj.run_make(force=True)
    """

    def __init__(
        self, makefile_root_path: Optional[PosixPath] = None, makefile_name: None = None
    ) -> None:
        """
        Call the make file.

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
            makefile_root_path = get_caller_dir()
        self.makefile_root_path = Path(makefile_root_path)
        logging.debug("self.makefile_root_path set to %s", makefile_root_path)

        self.makefile_name = makefile_name

        self.makefile_path = get_makefile_path(
            self.makefile_root_path, self.makefile_name
        )
        self.exec_name = get_exec_name(self.makefile_path)
        self.submitter = LocalSubmitter(self.makefile_root_path)

    def run_make(self, force: bool = False) -> None:
        """
        Execute the makefile.

        If an executable is found, nothing will be done unless 'force'
        is set to True

        Parameters
        ----------
        force : bool
            If True, make clean will be called prior to make
        """
        # If force: Run clean so that `made` returns false
        if force:
            self.run_clean()

        # Check if already made
        made = self.makefile_root_path.joinpath(self.exec_name).is_file()

        # Do nothing if already made
        if not made:
            make_str = (
                "make"
                if self.makefile_name is None
                else f"make -f {self.makefile_name}"
            )

            logging.info("Making the program")
            command = f"{make_str}"
            self.submitter.submit_command(command)

    def run_clean(self) -> None:
        """Run make clean."""
        make_str = (
            "make" if self.makefile_name is None else f"make -f {self.makefile_name}"
        )

        logging.info("Running make clean")
        command = f"{make_str} clean"
        self.submitter.submit_command(command)
