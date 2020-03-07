"""Contains the BOUT runner class."""


from bout_runners.database.database_utils import tables_created
from bout_runners.database.database_creator import create_database
from bout_runners.database.database_utils import get_db_path


class BoutRunner:
    """
    Executes the command for submitting a run.

    FIXME: Add variables and attributes

    FIXME: Add examples
    """

    def __init__(self,
                 executor,
                 bookkeeper):
        """
        FIXME

        Parameters
        ----------
        FIXME
        """
        # Set member data
        self.executor = executor
        self.bookkeeper = bookkeeper

        # Get database
        db_path = get_db_path(project_path=self.bout_paths.project_path,
                              database_root_path=database_root_path)

    # FIXME: YOU ARE HERE 2. It should be
    #  renamed as the name is already in use in the creator
    # FIXME: Submitted time is different from start and end
    # FIXME: Should pid be used as well?
    def create_parameter_tables(project_path):
        """
        Create one table per section in BOUT.settings and one join table.

        Parameters
        ----------
        project_path : Path
            Path to the project
        """
        # FIXME: Run settings run is from runner_utils
        settings_path = run_settings_run(project_path,
                                         bout_inp_src_dir=None)
        parameter_dict = obtain_project_parameters(settings_path)
        parameter_dict_as_sql_types = \
            cast_parameters_to_sql_type(parameter_dict)
        database.create_parameter_tables(parameter_dict_as_sql_types)

    def run(self):

        if not tables_created(self.database):
            db_path = self.database.database_path.parent
            create_database(
                project_path=self.bout_paths.project_path,
                database_root_path=db_path)

        self.database.store_data_from_run(
            self,
            self.processor_split.number_of_processors)

        if tables_created(self.database) and not settings_run:
            self.database.update_status()
