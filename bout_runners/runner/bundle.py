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
        # FIXME: Can loop through nested list...[[we,run,together],[wait_for]]?

        # Populate the waiting_for_dict
        waiting_for_dict = {None: list()}
        for group in run_group:
            # Add the pre_hooks
            for pre_hook in pre_hooks:
                if pre_hook.waiting_for_id in waiting_for_dict:
                    waiting_for_dict[pre_hook.waiting_for_id].append(pre_hook)
                else:
                    waiting_for_dict[pre_hook.waiting_for_id] = [pre_hook]

            # Add the run
            if pre_hook.waiting_for_id in waiting_for_dict:
                waiting_for_dict[run_group.run.waiting_for_id].append(run_group.run)
            else:
                waiting_for_dict[run_group.run.waiting_for_id] = [run_group.run]

            # Add the post_hooks
            for post_hook in post_hooks:
                if post_hook.waiting_for_id in waiting_for_dict:
                    waiting_for_dict[post_hook.waiting_for_id].append(post_hook)
                else:
                    waiting_for_dict[post_hook.waiting_for_id] = [post_hook]

        # FIXME: Enable parallel execution on single machine, user can specify max nodes
        # FIXME: PBS system may have this functionality built-in already (check old
        #        bout_runners)
        # FIXME: Add monitor: Execute the next in line when pid has finished. If not
        #        success -> broken chain, but the rest can continue
        # multiprocessing.Queue([maxsize])
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue
        # NOTE: An element may wait for more than one element
        # Upon submitting
        submission_pid = dict()
        ...
        # NOTE: object_id is the object_id of either pre_hook, run_group.run or
        post_hook

        # Remove element from queue on submission
        waiting_for_dict[None].remove(submission_id[completed_pid])



        submission_pid[object_id] = pid
        # FIXME: Find the inverse of this dict as well

        # Assume monitoring found that one of the runs were done
        # Put those marked as waiting for the completed pid in the None key of
        # waiting_for_dict
        waiting_for_dict[None].\
            append(waiting_for_dict.pop(submission_id[completed_pid]))
        # If an element waits for several ids
        for key in waiting_for_dict.keys():
            if type(key) == list:
                if submission_id[completed_pid] in key:
                    if len(key) == 1:
                        waiting_for_dict[None].\
                            append(waiting_for_dict.pop(submission_id[completed_pid]))
                    else:
                        new_key = key.copy()
                        new_key.remove(submission_id[completed_pid])
                        waiting_for_dict[new_key] = waiting_for_dict.pop(key)

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
