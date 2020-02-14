"""Contains unittests for the base runner."""


import shutil
import pytest
from bout_runners.runners.base_runner import BoutPaths
from bout_runners.runners.base_runner import RunParameters
from bout_runners.runners.base_runner import BoutRunner


@pytest.fixture(scope='function', name='copy_bout_inp')
def fixture_copy_bout_inp():
    """
    Copy BOUT.inp to a temporary directory.

    Returns
    -------
    BoutInpCopier.copy_inp_path : function
        Function which copies BOUT.inp and returns the path to the
        temporary directory
    """

    # We store the directories to be removed in a list, as lists are
    # mutable irrespective of the scope of their definition
    tmp_dir_list = []

    def copy_inp_path(project_path, tmp_path_name):
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
        bout_inp_path = project_path.joinpath('data', 'BOUT.inp')

        tmp_bout_inp_dir = project_path.joinpath(tmp_path_name)
        tmp_bout_inp_dir.mkdir(exist_ok=True)
        tmp_dir_list.append(tmp_bout_inp_dir)

        shutil.copy(bout_inp_path,
                    tmp_bout_inp_dir.joinpath('BOUT.inp'))

        return tmp_bout_inp_dir

    yield copy_inp_path

    for tmp_dir_path in tmp_dir_list:
        shutil.rmtree(tmp_dir_path)


def test_bout_path(yield_conduction_path, copy_bout_inp):
    """
    Test that BoutPath is copying BOUT.inp.

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


def test_run_parameters():
    """Test that the RunParameters are setting the parameters."""
    run_parameters = RunParameters({'global': {'append': False},
                                    'mesh':  {'nx': 4}})
    expected_str = 'append=False mesh.nx=4 '
    assert run_parameters.run_parameters_str == expected_str
    with pytest.raises(AttributeError):
        run_parameters.run_parameters_str = 'foo'


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
