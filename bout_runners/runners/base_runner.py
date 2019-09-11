import logging
from bout_runners.make.make import MakeProject
from bout_runners.utils.subprocesses_functions import run_subprocess


logger = logging.getLogger(__name__)

# FIXME: Rudimentary runner to create database


def single_run(path, nproc, options):
    make = MakeProject(path)
    make.run_make()

    # FIXME: mpirun obtained from getmpirun in boututils. Still needed?
    command = f'mpirun -np {nproc} ./{make.exec_name} {options}'

    run_subprocess(command, path=path)
