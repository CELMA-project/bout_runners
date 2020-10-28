"""Contains fixtures for building executables."""


from distutils.dir_util import copy_tree, remove_tree
from pathlib import Path
from typing import Iterator, Tuple

import pytest

from bout_runners.make.make import Make


@pytest.fixture(scope="function")
def make_make_object(yield_bout_path: Path) -> Iterator[Tuple[Make, Path]]:
    """
    Set up and tear down the make-object.

    In order not to make collisions with the global fixture which makes the
    `conduction` program, this fixture copies the content of the `conduction`
    directory to a `tmp` directory, which is removed in the teardown.

    This fixture calls make_obj.run_clean() before the yield statement.

    Parameters
    ----------
    yield_bout_path : Path
        Path to the BOUT++ repository. See the yield_bout_path fixture for more details

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    exec_file : Path
        The path to the executable

    See Also
    --------
    tests.bout_runners.conftest.get_bout_directory : Fixture returning the BOUT++ path
    """
    # Setup
    bout_path = yield_bout_path
    project_path = bout_path.joinpath("examples", "conduction")
    tmp_path = project_path.parent.joinpath("tmp_make")

    copy_tree(str(project_path), str(tmp_path))

    exec_file = tmp_path.joinpath("conduction")

    make_obj = Make(makefile_root_path=tmp_path)
    make_obj.run_clean()

    yield make_obj, exec_file

    # Teardown
    remove_tree(str(tmp_path))


@pytest.fixture(scope="session")
def make_project(yield_conduction_path: Path) -> Iterator[Path]:
    """
    Set up and tear down the Make object.

    The method calls make_obj.run_clean() before and after the yield statement

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example
        See the yield_conduction_path for more details

    Yields
    ------
    project_path : Path
        The path to the conduction example
    """
    # Setup
    project_path = yield_conduction_path

    make_obj = Make(makefile_root_path=project_path)
    make_obj.run_make()

    yield project_path

    # Teardown
    make_obj.run_clean()
