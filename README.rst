|bout_runners|

.. |bout_runners| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/logo_full.svg
    :alt: bout runners

=====

|lint| |test| |docker| |codecov| |docs|

|pypi| |python| |license|

|bandit| |code_style| |mypy|

.. |lint| image:: https://github.com/CELMA-project/bout_runners/workflows/Lint/badge.svg?branch=master
    :alt: lint status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Lint%22

.. |test| image:: https://github.com/CELMA-project/bout_runners/workflows/Test/badge.svg?branch=master
    :alt: test status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Test%22

.. |docker| image:: https://github.com/CELMA-project/bout_runners/workflows/Docker/badge.svg?branch=master
    :alt: docker build status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Docker%22

.. |codecov| image:: https://codecov.io/gh/CELMA-project/bout_runners/branch/master/graph/badge.svg
    :alt: codecov percentage
    :target: https://codecov.io/gh/CELMA-project/bout_runners

.. |docs| image:: https://readthedocs.org/projects/bout-runners/badge/?version=latest
    :alt: documentation status
    :target: https://bout-runners.readthedocs.io/en/latest/?badge=latest

.. |pypi| image:: https://badge.fury.io/py/bout-runners.svg
    :alt: pypi package number
    :target: https://pypi.org/project/bout-runners/

.. |python| image:: https://img.shields.io/badge/python->=3.6-blue.svg
    :alt: supported python versions
    :target: https://www.python.org/

.. |license| image:: https://img.shields.io/badge/license-LGPL--3.0-blue.svg
    :alt: licence
    :target: https://github.com/CELMA-project/bout_runners/blob/master/LICENSE

.. |code_style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :alt: code standard
    :target: https://github.com/psf/black

.. |mypy| image:: http://www.mypy-lang.org/static/mypy_badge.svg
    :alt: checked with mypy
    :target: http://mypy-lang.org/

.. |bandit| image:: https://img.shields.io/badge/security-bandit-yellow.svg
    :alt: security status
    :target: https://github.com/PyCQA/bandit

Manage your |BOUT++|_ runs through python

Overview
--------

``BOUT Runners`` is a package to manage and orchestrate your |BOUT++|_ runs.
More specifically it's a tool:

- Which automatically makes your project
- Where you can override default parameters, and parameters found in ``BOUT.inp``
- Where you can submit a single run, or even a chain of runs either locally, or to a cluster
- Which automatically stores parameters and other metadata in a database (inspired by |sacred|_)
- Logs the entire process

Read the full documentation_ at ReadTheDocs_

.. |sacred| replace:: ``sacred``
.. _sacred: https://github.com/IDSIA/sacred
.. _ReadTheDocs: https://readthedocs.org

Getting Started
---------------

The simplest way to use ``BOUT Runners`` is by executing the following script in the root of your project directory (usually where your ``Makefile`` resides).
In this example we are using ``BOUT-dev/examples/conduction`` as the root

.. code:: python

   from bout_runners.runner.bout_runner import BoutRunner
   BoutRunner().run()

The metadata from all the runs from this project can be found by executing

.. code:: python

    from bout_runners.metadata.status_checker import StatusChecker
    status_checker = StatusChecker()
    status_checker.check_and_update_status()
    from bout_runners.metadata.metadata_reader import MetadataReader
    metadata_reader = MetadataReader()
    metadata = metadata_reader.get_all_metadata()

The ``metadata`` variable is a ``DataFrame``, and contains the following table

+---+--------+-------------------+----------------------------+---------------------+---------------------+----------------------------+-----------------------------+----------------+------------------------------------------+-------------------------------------+-----------------------------------------------+------------------------------------------+---------------------------------------------+---------------+-------------------+-------------------------------------------------+--------------------+------------------------+--------------------+------------+------------+--------------------+-------------+-------------------+------------------+----------------+-----------------------+---------------------+------------------+-----------------+-------------------+-------------------+-------------+-------------+------------------------------------+----------------------------------------+---------+--------------------+--------------------+--------------+--------------+-------------------------+---------+---------+---------+------------------------+-------------------+-----------------------+-----------------------+-----------+---------------------+----------------+---------------+--------------+-----------------------+---------------+---------------------+------------------+-----------------+-------------------+--------------------+-----------------+----------------+---------------+------------------------+----------------+----------------------+-------------------+------------------+--------------------+---------------------+----------------------+-------------+------------------------+----------------------------------------+-----------------+------------------+------------------------------------+---------------------+-------------+---------------------+------------+-----------------------+-------------------------+----------------+---------------+-------------+--------------------+-------------+-----------------------+-------------+---------------------+-------------------+--------------------------+-----------------------+----------------------------+---------------------------+---------------------+------------------+-----------------------+---------------------+--------------------+-------------------------------------+-------------------+----------------+------------------+---------+
|   | run.id | run.latest_status | run.name                   | run.start_time      | run.stop_time       | run.submitted_time         | all_boundaries.evolve_bndry | conduction.chi | file_modification.bout_git_sha           | file_modification.bout_lib_modified | file_modification.project_executable_modified | file_modification.project_git_sha        | file_modification.project_makefile_modified | global.append | global.async_send | global.datadir                                  | global.dump_format | global.dump_on_restart | global.incintshear | global.mxg | global.myg | global.non_uniform | global.nout | global.optionfile | global.periodicx | global.restart | global.restart_format | global.settingsfile | global.stopcheck | global.timestep | global.twistshift | global.wall_limit | global.zmax | global.zmin | input.transform_from_field_aligned | mesh.calcparallelslices_on_communicate | mesh.dy | mesh.extrapolate_x | mesh.extrapolate_y | mesh.ixseps1 | mesh.ixseps2 | mesh.maxregionblocksize | mesh.nx | mesh.ny | mesh.nz | mesh.paralleltransform | mesh.staggergrids | mesh.symmetricglobalx | mesh.symmetricglobaly | mesh.type | mesh_ddz.fft_filter | output.enabled | output.floats | output.flush | output.flushfrequency | output.guards | output.init_missing | output.openclose | output.parallel | output.shiftinput | output.shiftoutput | restart.enabled | restart.floats | restart.flush | restart.flushfrequency | restart.guards | restart.init_missing | restart.openclose | restart.parallel | restart.shiftinput | restart.shiftoutput | solver.adams_moulton | solver.atol | solver.cvode_max_order | solver.cvode_stability_limit_detection | solver.diagnose | solver.func_iter | solver.is_nonsplit_model_diffusive | solver.max_timestep | solver.maxl | solver.min_timestep | solver.mms | solver.mms_initialise | solver.monitor_timestep | solver.mxorder | solver.mxstep | solver.nout | solver.output_step | solver.rtol | solver.start_timestep | solver.type | solver.use_jacobian | solver.use_precon | solver.use_vector_abstol | split.number_of_nodes | split.number_of_processors | split.processors_per_node | system_info.machine | system_info.node | system_info.processor | system_info.release | system_info.system | system_info.version                 | t.bndry_all       | t.evolve_bndry | t.function       | t.scale |
+===+========+===================+============================+=====================+=====================+============================+=============================+================+==========================================+=====================================+===============================================+==========================================+=============================================+===============+===================+=================================================+====================+========================+====================+============+============+====================+=============+===================+==================+================+=======================+=====================+==================+=================+===================+===================+=============+=============+====================================+========================================+=========+====================+====================+==============+==============+=========================+=========+=========+=========+========================+===================+=======================+=======================+===========+=====================+================+===============+==============+=======================+===============+=====================+==================+=================+===================+====================+=================+================+===============+========================+================+======================+===================+==================+====================+=====================+======================+=============+========================+========================================+=================+==================+====================================+=====================+=============+=====================+============+=======================+=========================+================+===============+=============+====================+=============+=======================+=============+=====================+===================+==========================+=======================+============================+===========================+=====================+==================+=======================+=====================+====================+=====================================+===================+================+==================+=========+
| 0 | 1      | complete          | 2020-06-09_07-14-39_981268 | 2020-06-09 07:14:53 | 2020-06-09 07:14:56 | 2020-06-09 07:14:52.943486 | 0                           | 1.0            | 22c888152e49003c34723a2638504aabc25d87ba | 2020-02-03 20:03:02.000000          | 2020-06-09 07:14:39.631118                    | 22c888152e49003c34723a2638504aabc25d87ba | 2020-02-03 19:48:41.000000                  | 0             | 0                 | /root/bout-dev/examples/conduction/settings_run | nc                 | 1                      | 0                  | 0          | 2          | 1                  | 0           | bout.inp          | 0                | 0              | nc                    | bout.settings       | 0                | 0.1             | 0                 | -1                | 1           | 0           | 1                                  | 1                                      | 0.2     | 0                  | 0                  | -1           | -1           | 64                      | 1       | 100     | 1       | identity               | 0                 | 1                     | 1                     | bout      | 0                   | 1              | 0             | 1            | 1                     | 1             | 0                   | 1                | 0               | 0                 | 0                  | 1               | 0              | 1             | 1                      | 1              | 0                    | 1                 | 0                | 0                  | 0                   | 0                    | 1e-12       | -1                     | 0                                      | 0               | 0                | 1                                  | -1                  | 5           | -1                  | 0          | 0                     | 0                       | -1             | 500           | 0           | 0.1                | 1e-05       | -1                    | cvode       | 0                   | 0                 | 0                        | 1                     | 1                          | 1                         | x86_64              | 0f17950a0dcc     |                       | 4.19.76-linuxkit    | Linux              | #1 SMP Tue May 26 11:42:35 UTC 2020 | dirichlet_o4(0.0) | 0              | gauss(y-pi, 0.2) | 1.0     |
+---+--------+-------------------+----------------------------+---------------------+---------------------+----------------------------+-----------------------------+----------------+------------------------------------------+-------------------------------------+-----------------------------------------------+------------------------------------------+---------------------------------------------+---------------+-------------------+-------------------------------------------------+--------------------+------------------------+--------------------+------------+------------+--------------------+-------------+-------------------+------------------+----------------+-----------------------+---------------------+------------------+-----------------+-------------------+-------------------+-------------+-------------+------------------------------------+----------------------------------------+---------+--------------------+--------------------+--------------+--------------+-------------------------+---------+---------+---------+------------------------+-------------------+-----------------------+-----------------------+-----------+---------------------+----------------+---------------+--------------+-----------------------+---------------+---------------------+------------------+-----------------+-------------------+--------------------+-----------------+----------------+---------------+------------------------+----------------+----------------------+-------------------+------------------+--------------------+---------------------+----------------------+-------------+------------------------+----------------------------------------+-----------------+------------------+------------------------------------+---------------------+-------------+---------------------+------------+-----------------------+-------------------------+----------------+---------------+-------------+--------------------+-------------+-----------------------+-------------+---------------------+-------------------+--------------------------+-----------------------+----------------------------+---------------------------+---------------------+------------------+-----------------------+---------------------+--------------------+-------------------------------------+-------------------+----------------+------------------+---------+



Prerequisites
-------------

- Python_ (versions above ``3.6`` are supported)
- |BOUT++|_, which can installed as stated in the manual_, or by using |bout_install|_

.. _Python: https://www.python.org
.. _manual: https://bout-dev.readthedocs.io/en/latest/user_docs/installing.html#installing-dependencies
.. |bout_install| replace:: ``bout_install``
.. _bout_install: https://pypi.org/project/bout-install/

Installing
----------

The package can be installed from ``pip``

.. code:: sh

   pip install bout-runners

Or from source

.. code:: sh

   python setup.py install

After installation you can optionally call

.. code:: sh

    bout_runners_config

in order to setup the path to your ``BOUT++`` installation and to configure other parameters like logging.

Running the tests
-----------------

The test suite can be executed through ``pytest`` or through ``codecov pytest-cov``.
Installation through

.. code:: sh

    pip install pytest

or

.. code:: sh

    pip install codecov pytest-cov


and run with

.. code:: sh

    pytest

or

.. code:: sh

    pytest --cov=./

respectively

Contributing
------------

Please read |CONTRIBUTING.rst|_ for details about how to contribute.

.. |CONTRIBUTING.rst| replace:: ``CONTRIBUTING.rst``
.. _CONTRIBUTING.rst: https://github.com/CELMA-project/bout_runners/blob/master/.github/CONTRIBUTING.rst

Authors
-------

* **Michael Løiten** - *Initial work*

See also the list of contributors_ who participated in this project.

.. _contributors: https://github.com/CELMA-project/bout_runners/contributors

License
-------

This project is licensed under the ``GNU LESSER GENERAL PUBLIC LICENSE`` - see the LICENSE_ file for details

.. _LICENSE: https://github.com/CELMA-project/bout_runners/blob/master/LICENSE

Acknowledgments
---------------

* The `BOUT++ team`_ for fast and accurate response on the `BOUT++ issue tracker`_ and slack-channel_

.. _BOUT++ team: http://boutproject.github.io/about/
.. _BOUT++ issue tracker: https://github.com/boutproject/BOUT-dev/issues
.. _slack-channel: https://bout-project.slack.com/

.. _documentation: https://bout-runners.readthedocs.io/en/latest/
.. |BOUT++| replace:: ``BOUT++``
.. _BOUT++: http://boutproject.github.io
