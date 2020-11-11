.. _submittersTag:

Submitters
**********

``BOUT Runners`` makes it possible to manage runs locally, or to submit runs on a cluster.
A submitter factory therefore implements the method ``get_submitter()`` which automatically detects whether or not the python program has been executed on a cluster.
If no cluster is found all jobs will be submitted as ``LocalSubmitters``.
If a cluster is found the jobs will be submitted to the cluster.

Default parameters for the jobs can be set in ``submitters.ini`` (the path to this file can be set with the ``bout_runners_config`` executable).

LocalSubmitter
==============

If a job is submitted with a ``LocalSubmitter``, the ``BoutRunner`` object will submit all nodes which does not have any dependencies (i.e. other nodes with edges pointing to the node under consideration) in parallel using the ``subprocess`` module.
It will then monitor the runs and submit subsequent nodes only when all nodes at the current order has finished.


Cluster submitters
==================

Currently only the ``PBSSubmitter`` and ``SLURMSubmitter`` is implemented.
As ``PBS`` and ``SLURM`` has their own way of handling dependencies between jobs ``BoutRunner`` will defer the job to the cluster schedulers.

.. note::

    Clusters will usually reject jobs that state they depend on jobs that have already finished.
    Therefore, any job submitted using ``submitter.submit_command(command)`` will onlye be released to the cluster when ``submitter.release()`` is called.
    This is taken care of if you use ``BoutRunner.run()``.
