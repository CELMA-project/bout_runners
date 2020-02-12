"""Contains unittests for the base runner."""


import shutil
import pytest
from bout_runners.runners.base_runner import BoutPaths
from bout_runners.runners.base_runner import BoutRunner


@pytest.fixture(scope='function')
def copy_bout_inp():
    """
    Copy BOUT.inp to a temporary directory.

    Returns
    -------
    BoutInpCopier.copy_inp_path : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory
    """

    class BoutInpCopier:
        """Class which encapsulates the tmp_path_name."""

        def __init__(self):
            """Declare self.tmp_path_name."""
            self.tmp_bout_inp_dir = None

        def copy_inp_path(self, project_path, tmp_path_name):
            """
            Copy BOUT.inp to a temporary directory.

            Parameters
            ----------
            project_path : Path
                Root path to the project
            tmp_path_name : str
                Name of the temporary directory

            Returns
            -------
            tmp_bout_inp_dir : Path
                Path to the temporary directory
            """
            tmp_path_name = tmp_path_name
            bout_inp_path = project_path.joinpath('data', 'BOUT.inp')

            tmp_bout_inp_dir = project_path.joinpath(tmp_path_name)
            tmp_bout_inp_dir.mkdir(exist_ok=True)
            self.tmp_bout_inp_dir = tmp_bout_inp_dir

            shutil.copy(bout_inp_path,
                        tmp_bout_inp_dir.joinpath('BOUT.inp'))

            return tmp_bout_inp_dir

    copier = BoutInpCopier()
    yield copier.copy_inp_path

    shutil.rmtree(copier.tmp_bout_inp_dir)


def test_bout_path(yield_conduction_path, copy_bout_inp):
    """
    Test that it is possible to perform a single run.

    Parameters
    ----------
    yield_conduction_path : Path
        Path to the BOUT++ conduction example.
        See the fixture_get_conduction_path for more details
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory. See the copy_bout_inp fixture for
        more details.

    See Also
    --------
    tests.bout_runners.conftest.yield_conduction_path : Fixture which
    yields the path to the conduction example
    """
    project_path = yield_conduction_path
    tmp_path_name = 'tmp_BoutPath_test'

    tmp_path_dir = copy_bout_inp(project_path, tmp_path_name)

    # We remove the BOUT.inp to verify that BoutPaths copied the file
    tmp_path_dir.joinpath('BOUT.inp').unlink()

    bout_paths = BoutPaths(project_path=project_path,
                           bout_inp_dst_dir=tmp_path_name)

    assert project_path.joinpath(tmp_path_name, 'BOUT.inp').is_file()

    with pytest.raises(FileNotFoundError):
        bout_paths.bout_inp_src_dir = 'dir_without_BOUT_inp'


def test_single_run(copy_bout_inp, make_project):
    """
    Test that it is possible to perform a single run.

    Parameters
    ----------
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory. See the copy_bout_inp fixture for
        more details.
    make_project : Path
        Path to the project directory. See the make_project fixture
        for more details

    See Also
    --------
    tests.bout_runners.conftest.make_project : Fixture which makes
    the project
    """
    project_path = make_project
    tmp_path_name = 'tmp_settings_run'

    bout_inp_dir = copy_bout_inp(project_path, tmp_path_name)

    runner = BoutRunner(project_path=project_path)
    runner.set_inp_src(bout_inp_dir)
    runner.set_destination(bout_inp_dir)
    runner.set_split(1)
    runner.set_parameter_dict({'global': {'nout': 0}})
    runner.run()

    assert bout_inp_dir.joinpath('BOUT.dmp.0.nc').is_file()
