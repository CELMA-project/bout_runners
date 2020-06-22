"""Contains the bundle class."""


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
