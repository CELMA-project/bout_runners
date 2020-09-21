Runner
******

The ``BoutRunner`` class is the orchestrator of the run.
Its responsibility is to execute the nodes of the :ref:`the run graph<RunGraphTag>`, and to capture the metadata about the run.
It is composed of several other classes which each performs one specific task in the orchestration.
In its simplest form it can be called from a project directory (i.e. a directory with a ``make`` file and a directory which holds a ``BOUT.inp`` file) in the following way

.. code:: python

    from bout_runners.runner.bout_runner import BoutRunner
    BoutRunner().run()

In the example above ``BoutRunner`` and its classes will be populated with default values.
For a more fine grained control of the setup of the ``BOUT++`` run, see :ref:`Constructing the BoutRunSetup<ConstructTag>`.

Restarting
==========

If you do not care about run graphs, and you are only interested in restarting one run, you can simply execute the following script in your project directory

.. code:: python

    from bout_runners.runner.bout_runner import BoutRunner
    runner = BoutRunner()
    runner.run()  # This will execute the first run
    runner.run(restart_all=True)  # This will restart the run

.. note::

    The code above will not overwrite any files.
    The new dump files can be found in the directory ``<name_of_BOUT_inp_directory>_restart_<restart_number>``.

.. note::

    You can also make a graph of runs, where one of the nodes is a restart of the other.
    See :ref:`chaining restarts<ChainRestarts>` for details.


.. _ConstructTag:

Constructing the BoutRunner
===========================

Although it's nice to run the scripts from the root of the project directory, it may in certain cases be too strict.
The following example shows how to get a more fine-grained control of the ``runner`` class.

.. _BoutRunSetup:

Constructing the BoutRunSetup
-----------------------------

This section describes how a ``BoutRunSetup`` object can be build from scratch.
The ``BoutRunSetup`` controls all aspects of a single ``BOUT++`` run.

.. note::

    There are two types of run which can be executed through the ``BoutRunner``:
    The "bout run" and the "function run".
    A "bout run" is a run where a ``BOUT++`` project is involved.
    A "function run" is a run where a python function is involved.
    The "function runs" can be used for pre- and post-processor for a "bout run" as described in :ref:`the run graph<RunGraphTag>`.

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
    from bout_runners.runner.bout_run_setup import BoutRunSetup
    from bout_runners.runner.run_graph import RunGraph
    from bout_runners.runner.run_group import RunGroup
    from bout_runners.runner.bout_runner import BoutRunner

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

.. note::

    Options without a section in ``BOUT.inp`` is should be listed under the ``'global'`` key.
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

    db_connector = DatabaseConnector('name_of_database',
                                      db_root_path=Path().joinpath('path', 'to', 'dir')

We are now ready to build the ``BoutRunSetup`` object

.. code:: python

    bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)

.. note::

    The ``BoutRunSetup`` needs the ``final_parameters`` in order to write the metadata to the database.

Constructing the RunGroup and RunGraph
--------------------------------------

As we are just interested in a single bout run in this example, we will treat the ``RunGroup`` and the ``RunGraph`` as abstract concepts.
You can read more about them and see more elaborate examples in :ref:`the run graph<RunGraphTag>`.

The ``BoutRunner`` accepts a ``RunGraph``.
The following code will create the ``RunGraph`` and populate it with a ``RunGroup`` which contains the ``BoutRunSetup``

.. code:: python

    run_graph = RunGraph()
    RunGroup(run_graph, bout_run_setup, name='my_bout_run')  # This will add the run group to the run_graph

Starting the run
----------------

We are now ready to build the ``BoutRunner`` object.

.. code:: python

    runner = BoutRunner(run_graph)

Finally, we are ready to submit the run

.. code:: python

    runner.run()
