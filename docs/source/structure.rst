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
   ├── docs                    # Documentation directory
   │         └── source              # Source files for the directory
   │             ├── _static         # Static documentation files
   │             ├── api             # API documentation
   │             └── examples        # Example usage documentation
   └── tests                   # Test suite directory
       ├── data                # Static test data
       ├── integration         # Integration tests package
       │         └── bout_runners    # Integration test for bout_runners
       │             └── runners     # Integration for the runner package
       ├── unit                # Unit test package
       │    └── bout_runners       # Unit tests for bout_runners
       │       ├── database        # Unit tests for the database package
       │       ├── executor        # Unit tests for the executor package
       │       ├── log             # Unit tests for the log package
       │       ├── make            # Unit tests for the make package
       │       ├── metadata        # Unit tests for the metadata package
       │       ├── parameters      # Unit tests for the parameters package
       │       ├── runner          # Unit tests for the runner package
       │       ├── submitter       # Unit tests for the submitter package
       │       └── utils           # Unit tests for the utils package
       └── utils               # Utils used for testing
