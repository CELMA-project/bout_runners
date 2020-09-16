.. _RunGraphTag:

The run graph
*************

Before proceeding two concepts needs to be clarified:
The ``RunGroup`` and the ``RunGraph``.

The ``RunGroup`` is a group of runs tied to one (and only one) ``BoutRunSetup``.
The core of the ``RunGroup`` is therefore the bout run, and zero or more pre- and postprocessors may accompany the bout run.

The ``RunGraph`` is the directed acyclic graph that ``BoutRunners`` will execute.
It's nodes contains instructions to be executed, whereas the edges describes it's dependencies.
The execution of all nodes pointing to a specific node must successfully complete before the instructions of the specific node is to be be submitted.
