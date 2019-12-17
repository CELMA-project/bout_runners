"""Contains the base runner."""


from bout_runners.make.make import MakeProject
from bout_runners.utils.subprocesses_functions import run_subprocess


def single_run(execute_from_path,
               bout_inp_dir=None,
               nproc=None,
               options=None):
    """
    Execute a BOUT++ run.

    Parameters
    ----------
    execute_from_path : None or Path or str
        Root path of make file
        If None, the path of the root caller of MakeProject will
        be called
    bout_inp_dir : None or Path or str
        Path to the BOUT.inp directory
        If None is specified, default path will be used
    nproc : int
        Number of processors
    options : str
        Command line options to use
    """
    make = MakeProject(execute_from_path)
    make.run_make()

    mpi_cmd = 'mpirun -np'
    nproc_str = nproc if nproc is not None else 1
    bout_inp_path_str =\
        f' -d {bout_inp_dir}' if bout_inp_dir is not None else ''
    options_str = f' {options}' if options is not None else ''

    command = f'{mpi_cmd} {nproc_str} ./{make.exec_name}' \
              f'{bout_inp_path_str}{options_str}'

    run_subprocess(command, path=execute_from_path)
