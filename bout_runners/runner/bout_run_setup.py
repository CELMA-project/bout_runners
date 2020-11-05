"""Contains the BoutRunSetup class."""

import logging
from typing import Optional

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.executor.executor import Executor
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from bout_runners.metadata.metadata_recorder import MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters


class BoutRunSetup:
    """
    Class for setting up the BOUT++ run.

    More specifically this class will connect the executor object with the run
    parameters and a database to store the results in

    Attributes
    ----------
    __executor : Executor
        Getter variable for executor
    __db_connector : DatabaseConnector
        Getter variable for db_connector
    __final_parameters : FinalParameters
        Getter variable for final_parameters
    __db_creator : DatabaseCreator
        Object used to create the database
    __metadata_recorder : MetadataRecorder
        Object used to record the metadata about a run
    executor : Executor
        Object used to execute the run
    bout_paths : BoutPaths
        The BoutPaths obtained through the get property
    db_connector : DatabaseConnector
        Object containing the database connection
    final_parameters : FinalParameters
        Object containing the parameters to use
    metadata_recorder : MetadataRecorder
        Object containing the metadata recorder
    submitter : AbstractSubmitter
        The submitter obtained through the get property

    Methods
    -------
    __create_schema()
        Create the schema

    Examples
    --------
    >>> run_setup = BoutRunSetup(executor, db_connector, final_parameters)
    >>> run_graph = RunGraph()
    >>> run_group = RunGroup(run_graph, run_setup)
    >>> runner = BoutRunner(run_graph)
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

        This constructor will also create the schema if it does not exist.

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
        logging.info("Start: Making a BoutRunSetup object")
        self.__executor = executor if executor is not None else Executor()
        self.__final_parameters = (
            final_parameters if final_parameters is not None else FinalParameters()
        )
        self.__db_connector = (
            db_connector
            if db_connector is not None
            else DatabaseConnector(
                name=self.__executor.exec_name,
                db_root_path=self.__executor.bout_paths.project_path,
            )
        )
        self.__db_creator = DatabaseCreator(self.db_connector)
        self.__metadata_recorder = MetadataRecorder(
            self.__db_connector, self.executor.bout_paths, self.final_parameters
        )

        if not self.__metadata_recorder.db_reader.check_tables_created():
            logging.info(
                "Creating schema as no tables were found in " "%s",
                self.__metadata_recorder.db_reader.db_connector.db_path,
            )
            self.__create_schema()
        logging.info("Done: Making a BoutRunSetup object")

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
    def bout_paths(self) -> BoutPaths:
        """
        Return the BoutPaths.

        Returns
        -------
        BoutPaths
            The BoutPaths
        """
        return self.executor.bout_paths

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

    @property
    def metadata_recorder(self) -> MetadataRecorder:
        """
        Get the properties of self.metadata_recorder.

        Returns
        -------
        self.__metadata_recorder : MetadataRecorder
            The object holding the metadata recorder
        """
        return self.__metadata_recorder

    @property
    def submitter(self) -> AbstractSubmitter:
        """
        Return the AbstractSubmitter.

        Returns
        -------
        AbstractSubmitter
            The submitter which will be used for submitting the job
        """
        return self.executor.submitter

    def __create_schema(self) -> None:
        """Create the schema."""
        final_parameters_dict = self.final_parameters.get_final_parameters()
        final_parameters_as_sql_types = self.final_parameters.cast_to_sql_type(
            final_parameters_dict
        )
        self.__db_creator.create_all_schema_tables(final_parameters_as_sql_types)
