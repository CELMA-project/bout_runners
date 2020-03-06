"""Module containing utils only used in the executor package."""


import logging
import bout_runners.executor


def run_settings_run(project_path, bout_inp_src_dir=None):
    """
    Perform a test run.

    Parameters
    ----------
    project_path : Path
        Path to the project
    bout_inp_src_dir : Path or None
        Path to the BOUT.inp file
        Will be set to `data/` of the `project_path` if not set

    Returns
    -------
    settings_path : Path
        Path to the settings file
    """
    bout_paths = bout_runners.executor.base_runner.BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=bout_inp_src_dir,
        bout_inp_dst_dir='settings_run')
    run_parameters = \
        bout_runners.executor.base_runner.RunParameters(
            {'global': {'nout': 0}})
    runner = bout_runners.executor.base_runner.BoutRunner(
        bout_paths=bout_paths,
        run_parameters=run_parameters)
    logging.info('Performing a run to obtaining settings in %s. '
                 'Please do not modify this directory',
                 bout_paths.bout_inp_dst_dir)

    settings_path = \
        bout_paths.bout_inp_dst_dir.joinpath('BOUT.settings')

    if not settings_path.is_file():
        runner.run(settings_run=True)

    return settings_path
