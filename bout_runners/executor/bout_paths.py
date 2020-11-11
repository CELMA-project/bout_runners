"""Contains the class setting the BOUT++ paths."""


import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from bout_runners.utils.file_operations import get_caller_dir


class BoutPaths:
    """
    Class which sets the paths.

    Attributes
    ----------
    __project_path : None or Path
        Getter and setter variable for project_path
    __bout_inp_src_dir : None or Path
        Getter and setter variable for bout_inp_src_dir
    __bout_inp_dst_dir : None or Path
        Getter and setter variable for bout_inp_dst_dir
    project_path : Path
        The root path of the project
    bout_inp_src_dir : Path
        The path to the BOUT.inp source directory
    bout_inp_dst_dir : Path
        The path to the BOUT.inp destination directory

    Methods
    -------
    _copy_files()
        Copy BOUT.inp from bout_inp_src_dir to bout_inp_dst_dir

    Examples
    --------
    >>> bout_paths = BoutPaths()
    >>> bout_paths.project_path
    Path(/root/BOUT-dev/examples/conduction)

    >>> bout_paths.bout_inp_src_dir
    Path(/root/BOUT-dev/examples/conduction/data)

    >>> bout_paths.bout_inp_dst_dir
    Path(/root/BOUT-dev/examples/conduction/2020-02-12_21-59-00_227295)

    >>> bout_paths.bout_inp_dst_dir = 'foo'
    Path(/root/BOUT-dev/examples/conduction/foo)
    """

    def __init__(
        self,
        project_path: Optional[Union[Path, str]] = None,
        bout_inp_src_dir: Optional[Union[Path, str]] = None,
        bout_inp_dst_dir: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Set the paths.

        Parameters
        ----------
        project_path : None or Path or str
            Root path of make file
            If None, the path of the path of the root caller will be used
        bout_inp_src_dir : None or str or Path
            The path to the BOUT.inp source directory (relative to self.project_path)
            If None, data will be used
        bout_inp_dst_dir : None or str or Path
            The path to the BOUT.inp bout_inp_dst_dir directory (relative to
            self.project_path)
            If None, the current time will be used
        """
        logging.info("Start: Making a BoutPahts object")
        # Declare variables to be used in the getters and setters
        # NOTE: When the variables will be set to absolute paths in the setters.
        #       Thus, Path() can be regarded as None
        self.__project_path: Path = Path()
        self.__bout_inp_src_dir: Path = Path()
        self.__bout_inp_dst_dir: Path = Path()

        # NOTE: type: ignore due to https://github.com/python/mypy/issues/3004
        # Set the project path
        self.project_path = project_path  # type: ignore

        # Set the bout_inp_src_dir
        self.bout_inp_src_dir = bout_inp_src_dir  # type: ignore

        # Set the bout_inp_dst_dir
        self.bout_inp_dst_dir = bout_inp_dst_dir  # type: ignore
        logging.info("Done: Making a BoutPahts object")

    @property
    def project_path(self) -> Path:
        """
        Set the properties of self.project_path.

        If None is specified, the path of the path of the root caller will be used

        Returns
        -------
        Path
            Absolute path to the root of make file
        """
        return self.__project_path

    @project_path.setter
    def project_path(self, project_path: Optional[Union[Path, str]]) -> None:
        if project_path is None:
            project_path = get_caller_dir()
        project_path = Path(project_path).absolute()
        self.__project_path = project_path
        logging.debug("self.project_path set to %s", project_path)

    @property
    def bout_inp_src_dir(self) -> Path:
        """
        Set the properties of bout_inp_src_dir.

        The setter will convert bout_inp_src_dir an absolute path (as the input is
        relative to the project path), check that the directory exists, and copy the
        BOUT.inp file to the bout_inp_dst_dir path (self.bout_inp_dst_dir)

        The input should be the path to the BOUT.inp source directory (relative to
        self.project_path)
        If None, "data" will be used

        Returns
        -------
        self.__bout_inp_src_dir : Path
            The absolute path to the BOUT.inp source directory

        Raises
        ------
        FileNotFoundError
            If no BOUT.inp file is found in the directory
        """
        return self.__bout_inp_src_dir

    @bout_inp_src_dir.setter
    def bout_inp_src_dir(self, bout_inp_src_dir: Optional[Union[Path, str]]) -> None:
        bout_inp_src_dir = (
            Path(bout_inp_src_dir) if bout_inp_src_dir is not None else Path("data")
        )

        self.__bout_inp_src_dir = self.project_path.joinpath(bout_inp_src_dir)

        if not self.__bout_inp_src_dir.joinpath("BOUT.inp").is_file():
            msg = f"No BOUT.inp file found in " f"{self.__bout_inp_src_dir}"
            logging.critical(msg)
            raise FileNotFoundError(msg)

        logging.debug("self.bout_inp_src_dir set to %s", self.__bout_inp_src_dir)

        # Copy file to bout_inp_dst_dir if set
        if self.bout_inp_dst_dir != Path():
            self._copy_files()

    @property
    def bout_inp_dst_dir(self) -> Path:
        """
        Set the properties of bout_inp_dst_dir.

        The setter will convert bout_inp_dst_dir an absolute path (as the input is
        relative to the project path), and copy BOUT.inp from self.bout_inp_src_dir
        to self.bout_inp_dst_dir

        The input should be the path to the BOUT.inp bout_inp_dst_dir directory
        (relative to self.project_path)
        If None, the current time will be used

        Returns
        -------
        Path
            Path to the destination directory
        """
        return self.__bout_inp_dst_dir

    @bout_inp_dst_dir.setter
    def bout_inp_dst_dir(self, bout_inp_dst_dir: Optional[Union[Path, str]]) -> None:
        time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        bout_inp_dst_dir = (
            Path(bout_inp_dst_dir) if bout_inp_dst_dir is not None else Path(time)
        )

        self.__bout_inp_dst_dir = self.project_path.joinpath(bout_inp_dst_dir)
        self.__bout_inp_dst_dir.mkdir(exist_ok=True, parents=True)
        logging.debug("self.bout_inp_dst_dir set to %s", self.__bout_inp_dst_dir)

        self._copy_files()

    def _copy_files(self) -> None:
        """Copy BOUT.inp from bout_inp_src_dir to bout_inp_dst_dir."""
        if self.bout_inp_src_dir != self.bout_inp_dst_dir:
            src = self.bout_inp_src_dir.joinpath("BOUT.inp")
            dst = self.bout_inp_dst_dir.joinpath(src.name)
            shutil.copy(src, dst)
            logging.debug("Copied %s to %s", src, dst)
