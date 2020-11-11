Project structure
*****************

The following aims to explain the project structure

..
   Note: Built with ``tree -d``, pre-cleaned with the dangerous
   ``find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf``

.. code::

   .
   ├── bout_runners            # The project directory
   │         ├── config              # Configuration files for paths and logging
   │         ├── database            # Package responsible for connect, read, write and create databases
   │         ├── executor            # Package responsible for executing the project
   │         ├── log                 # Package responsible for reading BOUT++ logs
   │         ├── make                # Package responsible for reading and calling Makefiles
   │         ├── metadata            # Package responsible for reading, writing and updating metadata about the runs
   │         ├── parameters          # Package responsible for reading and setting the run parameters
   │         ├── runner              # Package responsible for orchestrating executions and metadata
   │         ├── submitter           # Package responsible for submitting commands
   │         └── utils               # Package containing utilities
   ├── docker                  # Scripts for building the docker image
   │         ├── PBS                # Docker file for PBS (used in tests)
   │         └── SLURM              # Docker file for SLURM (used in tests)
   ├── docs                    # Documentation directory
   │         └── source              # Source files for the directory
   │             ├── _static         # Static documentation files
   │             ├── api             # API documentation
   │             └── examples        # Example usage documentation
   └── tests                   # Test suite directory


       ├── local               # Package containing the tests for local runs
       ├── integration         # Integration tests package
       │         └── bout_runners    # Integration test for bout_runners
       │             └── runners     # Integration for the runner package
       ├── unit                # Unit test package
       │    └── bout_runners       # Unit tests for bout_runners


       ├── data                # Static test data
       ├── fixtures            # Fixtures for the tests
       ├── local               # Package containing the tests for local runs
       │       ├── integration    # Local integration tests package
       │       │     └── runners     # Local integration for the runner package
       │       └── unit           # Local unit tests package
       │             ├── database    # Local unit tests for the database package
       │             ├── executor    # Local unit tests for the executor package
       │             ├── log         # Local unit tests for the log package
       │             ├── make        # Local unit tests for the make package
       │             ├── metadata    # Local unit tests for the metadata package
       │             ├── parameters  # Local unit tests for the parameters package
       │             ├── runner      # Local unit tests for the runner package
       │             ├── submitter   # Local unit tests for the submitter package
       │             └── utils       # Local unit tests for the utils package
       ├── pbs                 # Package containing the tests for PBS runs
       │       ├── integration    # PBS integration tests package
       │       │     └── runners     # PBS integration for the runner package
       │       └── unit           # PBS unit tests package
       │           ├── runners       # PBS unit tests for the runner package
       │           └── submitter     # PBS unit tests for the submitter package
       ├── slurm               # Package containing the tests for PBS runs
       │       ├── integration    # SLURM integration tests package
       │       │     └── runners     # SLURM integration for the runner package
       │       └── unit           # SLURM unit tests package
       │           ├── runners       # SLURM unit tests for the runner package
       │           └── submitter     # SLURM unit tests for the submitter package
       └── utils               # Utils used for testing
