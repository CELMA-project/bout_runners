import logging
from bout_runners.make.make import MakeProject
from bout_runners.utils.subprocesses_functions import run_subprocess

# FIXME: Add some logging, or remove the logger
logger = logging.getLogger(__name__)

# FIXME: Rudimentary runner to create database


def single_run(execute_from_path,
               bout_inp_dir=None,
               nproc=None,
               options=None):
    make = MakeProject(execute_from_path)
    make.run_make()

    # FIXME: mpirun obtained from getmpirun in boututils. Still needed?
    mpi_cmd = 'mpirun -np'
    nproc_str = nproc if nproc is not None else 1
    bout_inp_path_str =\
        f' -d {bout_inp_dir}' if bout_inp_dir is not None else ''
    options_str = f' {options}' if options is not None else ''

    command = f'{mpi_cmd} {nproc_str} ./{make.exec_name}' \
              f'{bout_inp_path_str}{options_str}'

    run_subprocess(command, path=execute_from_path)
