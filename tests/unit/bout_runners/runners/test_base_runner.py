import pytest
import shutil
from bout_runners.runners.base_runner import single_run


@pytest.fixture(scope='function')
def make_tmp_test_run_dir(make_project):
    """
    Makes a directory for testing a single run

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
    Tests that it is possible to perform a single run

    Parameters
    ----------
    make_tmp_test_run_dir : Path
        Path to the BOUT.inp directory. See the make_tmp_test_run_dir fixture
        for more details

    See Also
    --------
    make_tmp_test_run_dir : Fixture which makes a directory for the
    test run
    """

    bout_inp_dir = make_tmp_test_run_dir
    project_path = bout_inp_dir.parent

    single_run(execute_from_path=project_path,
               bout_inp_dir=bout_inp_dir,
               nproc=1,
               options='nout=0')

    assert bout_inp_dir.joinpath('BOUT.dmp.0.nc').is_file()
