"""Contains unittests for the base runner."""


import shutil
import pytest
from bout_runners.runners.base_runner import BoutRunner


@pytest.fixture(scope='function', name='make_tmp_test_run_dir')
def fixture_make_tmp_test_run_dir(make_project):
    """
    Make a directory for testing a single run.

    Parameters
    ----------
    make_project : Path
        Path to the project directory. See the make_project fixture
        for more details

    See Also
    --------
    tests.bout_runners.conftest.make_project : Fixture which makes
    the project
    """
    project_root = make_project
    bout_inp_path = project_root.joinpath('data', 'BOUT.inp')

    tmp_bout_inp_dir = project_root.joinpath('tmp_test_run')
    tmp_bout_inp_dir.mkdir(exist_ok=True)

    shutil.copy(bout_inp_path, tmp_bout_inp_dir.joinpath('BOUT.inp'))

    yield tmp_bout_inp_dir

    shutil.rmtree(tmp_bout_inp_dir)


def test_single_run(make_tmp_test_run_dir):
    """
    Test that it is possible to perform a single run.

    Parameters
    ----------
    make_tmp_test_run_dir : Path
        Path to the BOUT.inp directory

    See Also
    --------
    make_tmp_test_run_dir : Fixture which makes a directory for the
    test run
    """
    bout_inp_dir = make_tmp_test_run_dir
    project_path = bout_inp_dir.parent

    runner = BoutRunner(execute_from_path=project_path)
    runner.set_inp_src(bout_inp_dir)
    runner.set_destination(bout_inp_dir)
    runner.set_split(1)
    runner.set_options({'global': {'nout': 0}})
    runner.run()

    assert bout_inp_dir.joinpath('BOUT.dmp.0.nc').is_file()
