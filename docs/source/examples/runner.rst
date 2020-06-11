Although it's nice to run the scripts from the root of the project directory, it may in certain cases be too strict.
The following example shows how to get a more fine-grained control of the ``runner`` class.

First we import the necessary dependencies

.. code:: python

    from pathlib import Path
    from bout_runners.executor.bout_paths import BoutPaths
    from bout_runners.executor.executor import Executor
    from bout_runners.database.database_connector import DatabaseConnector
    from bout_runners.parameters.default_parameters import DefaultParameters
    from bout_runners.parameters.run_parameters import RunParameters
    from bout_runners.parameters.final_parameters import FinalParameters
    from bout_runners.submitter.local_submitter import LocalSubmitter

Next, we create the ``bout_paths`` object.
This object handles the path to the project.
It is also responsible for copying the ``BOUT.inp`` from a source directory to a destination directory, and will throw an error if the source ``BOUT.inp`` is not found

.. code:: python

    project_path = Path().joinpath('path', 'to', 'project')
    bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    bout_paths = BoutPaths(project_path=project_path,
                           bout_inp_src_dir=bout_inp_src_dir,
                           bout_inp_dst_dir=bout_inp_dst_dir)

We can also override the parameters in the ``BOUT.inp`` located in the destination directory by using the ``parameters`` package.
The ``parameters`` package contains the classes ``DefaultParameters``,  ``RunParameters`` and ``FinalParameters``.
The ``DefaultParameters`` obtains the default parameters by reading the ``BOUT.settings`` file. If none is present a ``settings_run`` with ``nout = 0`` will be executed.
The ``RunParameters`` accepts a dict which overrides the sections in ``BOUT.inp``.

.. Note:: Options without a section in ``BOUT.inp`` is should be listed under the ``'global'`` key.
    See the code below, where ``'nout'`` is listed under ``'global'``.

Finally, the ``FinalParameters`` synthesize the parameters from ``DefaultParameters`` and ``RunParameters``, where ``RunParameters`` will have precedence.
The ``FinalParameters`` will contain the parameters which will be used when executing the run.

.. code:: python

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({'global': {'nout': 0}})
    final_parameters = FinalParameters(default_parameters,
                                       run_parameters)

Next, we create an ``Executor`` instance.
This is responsible for submitting the command to the system which will carry out the run.
The ``submitter`` parameter accepts any submitters which inherits from  ``AbstractSubmitter`` which includes ``LocalSubmitter``, ``PBSSubmitter`` and ``SlurmSubmitter``.

.. code:: python

    submitter = LocalSubmitter(bout_paths.project_path)
    executor = Executor(
        bout_paths=bout_paths,
        submitter=submitter,
        run_parameters=run_parameters)

In addition, we need to know what database to write to.

.. code:: python

    db_connection = DatabaseConnector('name_of_database',
                                      db_root_path=Path().joinpath('path', 'to', 'dir')

We are now ready to build the ``BoutRunner`` object.

.. Note:: The ``BoutRunner`` needs the ``final_parameters`` in order to write the metadata to the database.

.. code:: python

    runner = BoutRunner(executor,
                        db_connection,
                        final_parameters)

Finally, we are ready to submit the run

.. code:: python

    runner.run()
