"""Contains unittests for the runner."""


from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.executor.executor import Executor


# FIXME: Make status object which updates the status
# FIXME: Needs update
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

    bout_paths = BoutPaths(project_path=project_path,
                           bout_inp_src_dir=bout_inp_dir,
                           bout_inp_dst_dir=bout_inp_dir)
    run_parameters = RunParameters({'global': {'nout': 0}})
    runner = Executor(bout_paths=bout_paths,
                      run_parameters=run_parameters)
    # FIXME: This test is failing. Possibly because bookeeper is
    #  messy, and should be refactored
    runner.run()

    assert bout_inp_dir.joinpath('BOUT.dmp.0.nc').is_file()
