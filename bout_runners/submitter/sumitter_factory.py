"""Contains the submitter factory."""


import logging
from bout_runners.submitter.local_submitter import LocalSubmitter


class SubmitterFactory:
    """Factory which returns the submitters."""

    @staticmethod
    def get_submitter(name):
        """
        Return a model.

        Parameters
        ----------
        name : str
            Name of the submitter to use

        Returns
        -------
        submitter : AbstractSubmitter
            The implemented submitter class
        """
        implemented = ('local',)

        if name == 'local':
            submitter = LocalSubmitter()
        else:
            msg = (f'{name} is not a valid submitter class, choose '
                   f'from {implemented}')
            raise NotImplementedError(msg)

        logging.debug('%s submitter selected', name)

        return submitter
