"""Contains the BOUT runner class."""


import logging
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.metadata_recorder.metadata_recorder import \
    MetadataRecorder


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    Attributes
    ----------
    self.__bout_paths : BoutPaths
        Object for the BOUT++ paths
    self.__settings_path : None or Path
        Path to the BOUT.settings file

    Methods
    -------
    run_parameters_run()
        Execute a run to obtain the default parameters.
    get_default_parameters()
        Return the default parameters from the settings file.

    Examples
    --------
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
    >>> final_parameters = FinalParameters(default_parameters)
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
                 executor,
                 database_connector,
                 final_parameters):
        """
        Set the member data.

        Parameters
        ----------
        executor : Executor
            Object executing the run
        database_connector : DatabaseConnector
            The connection to the databas
        final_parameters : FinalParameters
            The object containing the parameters which are going to
            be used in the run
        """
        # Set member data
        self.__executor = executor
        self.__database_creator = DatabaseCreator(database_connector)
        self.__final_parameters = final_parameters
        self.__metadata_recorder = \
            MetadataRecorder(database_connector,
                             executor.bout_paths,
                             self.__final_parameters)

    def create_schema(self):
        """Create the schema."""
        final_parameters_dict = \
            self.__final_parameters.get_final_parameters()
        final_parameters_as_sql_types = \
            self.__final_parameters. \
            cast_parameters_to_sql_type(final_parameters_dict)
        self.__database_creator.create_all_schema_tables(
            final_parameters_as_sql_types)

    def run(self):
        """Perform the run and capture data."""
        if not self.__metadata_recorder.database_reader.\
                check_tables_created():
            logging.info('Creating schema as no tables were found in '
                         '%s',
                         self.__metadata_recorder.database_reader.
                         database_connector.database_path)
            self.create_schema()

        run_id = self.__metadata_recorder.capture_new_data_from_run(
            self.__executor.submitter.processor_split)

        if run_id is None:
            self.__executor.execute()
        else:
            logging.warning('Run with the same configuration has been '
                            'executed before, see run with run_id %d',
                            run_id)
