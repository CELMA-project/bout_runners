"""Contains the BOUT runner class."""


import logging
from typing import Optional

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.executor.executor import Executor
from bout_runners.metadata.metadata_recorder import MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    Attributes
    ----------
    self.__executor : Executor
        Getter variable for executor
    self.__db_connector : DatabaseConnector
        Getter variable for db_connector
    self.__final_parameters : FinalParameters
        Getter variable for final_parameters
    self.__db_creator : DatabaseCreator
        Object used to create the database
    self.__metadata_recorder : MetadataRecorder
        Object used to record the metadata about a run
    self.executor : Executor
        Object used to execute the run
    self.db_creator : DatabaseCreator
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
    The easiest way to use BoutRunner is to run a script from the root directory of
    the project (i.e. where the `Makefile` and `data` directory are normally
    situated. The script can simply call

    >>> BoutRunner().run()

    and `BoutRunner` takes care of the rest.

    A more elaborate example where all the dependency objects are built manually:

    Import dependencies

    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.executor.executor import Executor
    >>> from bout_runners.database.database_connector import DatabaseConnector
    >>> from bout_runners.parameters.default_parameters import DefaultParameters
    >>> from bout_runners.parameters.run_parameters import RunParameters
    >>> from bout_runners.parameters.final_parameters import FinalParameters
    >>> from bout_runners.submitter.local_submitter import LocalSubmitter

    Create the `bout_paths` object

    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Create the input objects

    >>> default_parameters = DefaultParameters(bout_paths)
    >>> run_parameters = RunParameters({'global': {'nout': 0}})
    >>> final_parameters = FinalParameters(default_parameters,
    ...                                    run_parameters)
    >>> executor = Executor(
    ...     bout_paths=bout_paths,
    ...     submitter=LocalSubmitter(bout_paths.project_path),
    ...     run_parameters=run_parameters)
    >>> db_connection = DatabaseConnector('name_of_database', db_root_path=Path())

    Run the project

    >>> runner = BoutRunner(executor,
    ...                     db_connection,
    ...                     final_parameters)
    >>> runner.run()
    """

    def __init__(
        self,
        executor: Optional[Executor] = None,
        db_connector: Optional[DatabaseConnector] = None,
        final_parameters: Optional[FinalParameters] = None,
    ) -> None:
        """
        Set the member data.

        Parameters
        ----------
        executor : Executor or None
            Object executing the run
            If None, default parameters will be used
        db_connector : DatabaseConnector or None
            The connection to the database
            If None: Default database connector will be used
        final_parameters : FinalParameters or None
            The object containing the parameters which are going to be used in the run
            If None, default parameters will be used
        """
        # Set member data
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.__executor = executor if executor is not None else Executor()
        self.__final_parameters = (
            final_parameters if final_parameters is not None else FinalParameters()
        )
        self.__db_connector = (
            db_connector if db_connector is not None else DatabaseConnector()
        )
        self.__db_creator = DatabaseCreator(self.db_connector)
        self.__metadata_recorder = MetadataRecorder(
            self.__db_connector, self.executor.bout_paths, self.final_parameters
        )

    @property
    def executor(self) -> Executor:
        """
        Get the properties of self.executor.

        Returns
        -------
        self.__executor : Executor
            The executor object
        """
        return self.__executor

    @property
    def final_parameters(self) -> FinalParameters:
        """
        Get the properties of self.final_parameters.

        Returns
        -------
        self.__final_parameters : FinalParameters
            The object containing the parameters used in the run
        """
        return self.__final_parameters

    @property
    def db_connector(self) -> DatabaseConnector:
        """
        Get the properties of self.db_connector.

        Returns
        -------
        self.__db_connector : DatabaseConnector
            The object holding the database connection
        """
        return self.__db_connector

    def create_schema(self) -> None:
        """Create the schema."""
        final_parameters_dict = self.final_parameters.get_final_parameters()
        final_parameters_as_sql_types = self.final_parameters.cast_to_sql_type(
            final_parameters_dict
        )
        self.__db_creator.create_all_schema_tables(final_parameters_as_sql_types)

    def run(self, force: bool = False) -> None:
        """
        Perform the run and capture data.

        Parameters
        ----------
        force : bool
            Execute the run even if has been performed with the same parameters
        """
        if not self.__metadata_recorder.db_reader.check_tables_created():
            logging.info(
                "Creating schema as no tables were found in " "%s",
                self.__metadata_recorder.db_reader.db_connector.db_path,
            )
            self.create_schema()

        run_id = self.__metadata_recorder.capture_new_data_from_run(
            self.__executor.submitter.processor_split, force
        )

        if run_id is None:
            logging.info("Executing the run")
            self.executor.execute()
        else:
            logging.warning(
                "Run with the same configuration has been executed before, "
                "see run with run_id %d",
                run_id,
            )
            if force:
                logging.info("Executing the run as force==True")
                self.executor.execute()
