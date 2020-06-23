"""Contains the bundle class."""


from typing import Optional

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.executor.executor import Executor
from bout_runners.metadata.metadata_recorder import MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters


class RunSetup:
    """
    Class containing the setup of the run.

    FIXME
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


class RunGroup:
    """
    Class for building a run group.

    A run group must contain one, and only one, recipe for executing the project (
    called `run_setup`). It may consist of one or several pre-hooks (functions that
    will run prior to the project execution) together with one or several post-hooks
    (functions that will run after the project execution).

    The pre-hooks may be chained (i.e. executed in a series) to each other,
    but at least one pre-hook is chained to the project execution (i.e. when that
    pre-hook is finished the execution of the project will start). Similarly the
    post-hooks may be chained to each other, but at least one is chained to the
    project execution (i.e. it will start when the project execution is done).
    """

    def __init__(self, run_setup: RunSetup) -> None:
        """
        Run me.

        Parameters
        ----------
        run_setup : RunSetup
            The setup of the project execution
        """
        print(repr(run_setup))

    def add_pre_hook(self) -> None:
        """Run me."""

    def add_post_hook(self) -> None:
        """Run me."""


class Bundle:
    """Run a bundle."""

    def __init__(self,) -> None:
        """Construct lol."""

    def setup(self):
        """
        Call the pass function.

        # FIXME: Make documentation in readthedocs
        # FIXME: Make docstring documentation
        # FIXME: Make unittest

        pre_hook
        run
        post_hook
        next keyword?

        two names: one for pre, run and post, another one for group of the preceeding
        run_group - can only contain one run (recipe?) but unlimited pre and post hooks
        run_bundle - a collection of run groups

        chain must have a start and an end...implemented as waits_for and tag_id
        cannot be two of the same ids in a bundle
        add_pre_hook - as many as you like, group them (list so you can add self.hooks)
        add_run - not possible to add_pre_hook (unless you specify the tag_id)

        must be possible to write "for group in bundle"
        must be possible to submit hooks with individual runners
        can also make hooks wait for each other
        possible to attach process ids to hooks

        must have a nice string representation

        How should BoutRunners be rewritten?
        For the most, that just builds up the run object
        Ideally we would have something that takes the bundle as input and loops through
        If no bundle is entered, will create a run_group with the run, and added to
        bundle
        !!! Possible to refactor BoutRunners to run_group
        ??? Is bundle just a list??? - if so the chaining could be weird,
        bundle should be able to sort? Unless the sort is meaningless

        Currently BoutRunners will only take a bundle and run, what else...
        prune hooks if run has already been run
        """

    def teardown(self):
        """Call something that does not exist."""
