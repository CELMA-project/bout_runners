.. _RunGraphTag:

The run graph
*************

Before proceeding two concepts needs to be clarified:
The ``RunGroup`` and the ``RunGraph``.

The ``RunGroup`` is a group of runs tied to one (and only one) ``BoutRunSetup``.
The core of the ``RunGroup`` is therefore the bout run, and zero or more pre- and postprocessors may accompany the bout run.

The ``RunGraph`` is the directed acyclic graph that ``BoutRunners`` will execute.
It's nodes contains instructions to be executed, whereas the edges describes it's dependencies.
The execution of a node will not start until it's dependencies (i.e. nodes with edges directed towards the node under consideration) has successfully completed.
For more details see :ref:`submitters<submitters>`

``RunGraph`` is at a lower abstraction level than ``RunGroup``.
Thus everything you can do through ``RunGroup`` you could do through the ``RunGraph``.
However, the ``RunGroup`` automates the naming, the adding of nodes (with the appropriate ``submitter``) and the adding of edges.

.. _ChainRestarts:

Chaining restarts
=================

A typical use-case for using several ``RunGroup`` objects is to sequentially restart a run.
In the following example we will run a non-restart run, one run which restarts from the non-restart run without changing any parameters, and one run which restarts from the non-restart run and changes one parameter.

.. _basicBoutRunSetup:

Making the basic BoutRunSetup
-----------------------------

First we make the ``BoutRunSetup`` as we did in :ref:`constructing the BoutRunner<BoutRunSetup>` for details on how to make

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

    project_path = Path().joinpath('path', 'to', 'project')
    bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    bout_paths = BoutPaths(project_path=project_path,
                           bout_inp_src_dir=bout_inp_src_dir,
                           bout_inp_dst_dir=bout_inp_dst_dir)

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({'global': {'nout': 0}})
    final_parameters = FinalParameters(default_parameters,
                                       run_parameters)

    basic_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters)

    db_connector = DatabaseConnector('name_of_database',
                                      db_root_path=Path().joinpath('path', 'to', 'dir')

    basic_bout_run_setup = BoutRunSetup(basic_executor, db_connector, final_parameters)

    run_graph = RunGraph()
    name = 'my_restart_runs'
    basic_run_group = RunGroup(run_graph, basic_bout_run_setup, name=name)

Adding the restarts
-------------------

Next we add the restart run without changing any parameters

.. code:: python

    restart_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )
    file_state_restorer.add(
        restart_executor.bout_paths.bout_inp_dst_dir, force_mark_removal=True
    )

    restart_bout_run_setup = BoutRunSetup(
        restart_executor, db_connector, final_parameters
    )

    RunGroup(
        run_graph,
        restart_bout_run_setup,
        name=name,
        waiting_for=basic_run_group.bout_run_node_name,
    )

and the restart where we are changing the parameters are changing


.. code:: python

    new_run_parameters = RunParameters({"solver": {"adams_moulton": True}})
    new_final_parameters = FinalParameters(default_parameters, run_parameters)

    restart_with_changing_parameters_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=new_run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )
    file_state_restorer.add(
        restart_with_changing_parameters_executor.bout_paths.bout_inp_dst_dir,
        force_mark_removal=True,
    )

    BoutRunSetup(
        restart_with_changing_parameters_executor, db_connector, new_final_parameters
    )

    RunGroup(
        run_graph,
        restart_bout_run_setup,
        name=name,
        waiting_for=basic_run_group.bout_run_node_name,
    )

The dot graph (which can be viewed at for example http://www.webgraphviz.com) can be obtained by

.. code:: python

    run_graph.get_dot_string()

And will look like this

|restart_graph|

Finally we execute the runs

.. code:: python

    runner = BoutRunner(run_graph)
    runner.run()

.. note::

    Behind the scenes the ``BoutRunner`` object will do two things

    1. Change ``bout_paths.bout_inp_dst_dir`` to ``<name_of_BOUT_inp_directory>_restart_<restart_number>``
    2. Create new nodes which copies the the restart files from the ``restart_from`` directory to the new ``bout_paths.bout_inp_dst_dir``

    This ensures that restarting from previous runs will never overwrite any files.

With the alteration of the graph from ``BoutRunner``, the resulting graph will look as follows

|restart_graph_full|

.. |restart_graph| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/restart_graph.png
    :alt: Graph of chained restarts

.. |restart_graph_full| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/restart_graph_full.png
    :alt: Graph of chained restarts with alterations from BoutRunners

Pre- and post-processors
========================

To illustrate the use of pre- and post-processors, we will use the case where the equilibrium solution is homogeneous in the ``z``-direction.
We will therefore expand the dimensions and add noise to the restart files before restarting.

.. note::

    We are here assuming that the original run has ``nz=1``


We start by building the ``basic_bout_run_setup`` as we did in :ref:`Making the basic BoutRunSetup<basicBoutRunSetup>`.
Then, we add two post-processors: One post-processors which makes a plot, and another which expands the dimension.
Note that copying of restart files is handled in the ``BoutRunner`` object.

.. code:: python

    from boutdata.restart import resizeZ

    basic_run_group.add_post_processor({'function': my_plotting_function, 'args': None, 'kwargs':None})
    expanded_noise_restarts_dir = bout_paths.bout_inp_dst_dir.parent.joinpath(
        "expanded_noise_restarts"
    )
    kwargs = {'newNz': 16,
              'path': bout_paths.bout_inp_dst_dir,
              'output': expanded_noise_restarts_dir}
    expand_node_name = basic_run_group.add_post_processor({'function': resizeZ,
                                                           'args': None,
                                                           'kwargs':kwargs})

Next, we make a run group for the restart run, and add noise to the restart files as a pre-processing step

.. code:: python

    import shutil
    from boutdata.restart import addnoise

    # Create the RunGroup
    restart_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=expanded_noise_restarts_dir,
    )

    restart_bout_run_setup = BoutRunSetup(
        restart_executor, db_connector, final_parameters
    )

    restart_run_group = RunGroup(
        run_graph, restart_bout_run_setup, name=name
    )

    kwargs = {'path': expanded_noise_restarts_dir, 'scale': 1e-5}
    restart_run_group.add_pre_processor(
        {
            "function": return_none,
            "args": None,
            "kwargs": kwargs,
        },
        waiting_for=expand_node_name,
    )

The dot graph (which can be viewed at for example http://www.webgraphviz.com) can be obtained by

.. code:: python

    run_graph.get_dot_string()

And will look like this

|expand_graph|

Finally we execute the runs

.. code:: python

    runner = BoutRunner(run_graph)
    runner.run()

With the alteration of the graph from ``BoutRunner``, the resulting graph will look as follows

|expand_graph_full|

.. |expand_graph| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/expand_graph.png
    :alt: Graph of expanding restarts

.. |expand_graph_full| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/expand_graph_full.png
    :alt: Expand graph of chained restarts with alterations from BoutRunners
