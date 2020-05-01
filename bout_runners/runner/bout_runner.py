"""Contains the BOUT runner class."""


import logging
from bout_runners.executor.executor import Executor
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.metadata.metadata_recorder import \
    MetadataRecorder


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    Attributes
    ----------
    self.__executor : Executor
        Getter variable for executor
    self.__database_connector : DatabaseConnector
        Getter variable for database_connector
    self.__final_parameters : FinalParameters
        Getter variable for final_parameters
    self.__database_creator : DatabaseCreator
        Object used to create the database
    self.__metadata_recorder : MetadataRecorder
        Object used to record the metadata about a run
    self.executor : Executor
        Object used to execute the run
    self.database_creator : DatabaseCreator
        Object used to create the database
    self.final_parameters : FinalParameters
        Object containing the parameters to use

    Methods
    -------
    create_schema()
        Create the schema
    run(force)
        Execute the run

    Examples
    --------
    The easiest way to use BoutRunner is to run a script from the
    root directory of the project (i.e. where the `Makefile` and
    `data` directory are normally situated. The script can simply call
    >>> BoutRunner().run()

    and `BoutRunner` takes care of the rest.

    A more elaborate example where all the dependency objects are
    built manually:

    Import dependencies
    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.executor.executor import Executor
    >>> from bout_runners.database.database_connector import \
    ...     DatabaseConnector
    >>> from bout_runners.parameters.default_parameters import \
    ...     DefaultParameters
    >>> from bout_runners.parameters.run_parameters import \
    ...     RunParameters
    >>> from bout_runners.parameters.final_parameters import \
    ...     FinalParameters
    >>> from bout_runners.submitter.local_submitter import \
    ...     LocalSubmitter

    Create the `bout_paths` object
    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source',
    ... 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination',
    ... 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Create the input objects
    >>> run_parameters = RunParameters({'global': {'nout': 0}})
    >>> default_parameters = DefaultParameters(bout_paths)
    >>> final_parameters = FinalParameters(default_parameters,
    ...                                    run_parameters)
    >>> executor = Executor(
    ...     bout_paths=bout_paths,
    ...     submitter=LocalSubmitter(bout_paths.project_path),
    ...     run_parameters=run_parameters)
    >>> db_connection = DatabaseConnector('name_of_database')

    Run the project
    >>> runner = BoutRunner(executor,
    ...                     db_connection,
    ...                     final_parameters)
    >>> runner.run()
    """

    def __init__(self,
                 executor=None,
                 database_connector=DatabaseConnector(),
                 final_parameters=None):
        """
        Set the member data.

        Parameters
        ----------
        executor : Executor or None
            Object executing the run
            If None, default parameters will be used
        database_connector : DatabaseConnector
            The connection to the database
        final_parameters : FinalParameters or None
            The object containing the parameters which are going to
            be used in the run
            If None, default parameters will be used
        """
        # Set member data
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.__executor = \
            executor if executor is not None else Executor()
        self.__final_parameters = \
            final_parameters if final_parameters is not None else \
            FinalParameters()
        self.__database_connector = database_connector
        self.__database_creator = \
            DatabaseCreator(self.database_connector)
        self.__metadata_recorder = \
            MetadataRecorder(database_connector,
                             self.executor.bout_paths,
                             self.final_parameters)

    @property
    def executor(self):
        """
        Get the properties of self.executor.

        Returns
        -------
        self.__executor : Executor
            The executor object
        """
        return self.__executor

    @property
    def final_parameters(self):
        """
        Get the properties of self.final_parameters.

        Returns
        -------
        self.__final_parameters : FinalParameters
            The object containing the parameters used in the run
        """
        return self.__final_parameters

    @property
    def database_connector(self):
        """
        Get the properties of self.database_connector.

        Returns
        -------
        self.__database_connector : DatabaseConnector
            The object holding the database connection
        """
        return self.__database_connector

    def create_schema(self):
        """Create the schema."""
        final_parameters_dict = \
            self.final_parameters.get_final_parameters()
        final_parameters_as_sql_types = \
            self.final_parameters. \
            cast_parameters_to_sql_type(final_parameters_dict)
        self.__database_creator.create_all_schema_tables(
            final_parameters_as_sql_types)

    def run(self, force=False):
        """
        Perform the run and capture data.

        Parameters
        ----------
        force : bool
            Execute the run even if has been performed with the same
            parameters
        """
        if not self.__metadata_recorder.database_reader.\
                check_tables_created():
            logging.info('Creating schema as no tables were found in '
                         '%s',
                         self.__metadata_recorder.database_reader.
                         database_connector.database_path)
            self.create_schema()

        run_id = self.__metadata_recorder.capture_new_data_from_run(
            self.__executor.submitter.processor_split, force)

        if run_id is None:
            logging.info('Executing the run')
            self.executor.execute()
        else:
            logging.warning('Run with the same configuration has been '
                            'executed before, see run with run_id %d',
                            run_id)
            if force:
                logging.info('Executing the run as force==True')
                self.executor.execute()
