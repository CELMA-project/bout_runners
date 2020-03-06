"""Module containing the Bookkeeper class."""


from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader


class Bookkeeper:
    """
    Class for bookkeeping of the runs.

    Attributes
    ----------
    FIXME

    Methods
    -------
    FIXME

    FIXME: Add examples
    """

    def __init__(self, database_connector):
        """
        Set the database to use.

        Parameters
        ----------
        database_connector : DatabaseConnector
            The database connector
        """
        self.database_connector = database_connector
        # FIXME: Setters and getters, or maybe just private members?
        self.database_creator = DatabaseCreator(database_connector)
        self.database_writer = DatabaseWriter(database_connector)
        self.database_reader = DatabaseReader(database_connector)

    def _create_parameter_tables_entry(self, parameters_dict):
        """
        Insert the parameters into a the parameter tables.

        Parameters
        ----------
        parameters_dict : dict
            The dictionary on the form
            >>> {'section': {'parameter': 'value'}}

        Returns
        -------
        parameters_id : int
            The id key from the `parameters` table

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        parameter_sections = list(parameters_dict.keys())

        for section in parameter_sections:
            # Replace bad characters for SQL
            section_name = section.replace(':', '_')
            section_parameters = parameters_dict[section]
            section_id = \
                self.database_reader.get_entry_id(section_name,
                                                  section_parameters)
            if section_id is not None:
                section_id = self.database_writer.create_entry(
                    section_name,
                    section_parameters)

            parameters_foreign_keys[f'{section_name}_id'] = section_id

        # Update the parameters table
        parameters_id = \
            self.database_reader.get_entry_id('parameters',
                                              parameters_foreign_keys)
        if parameters_id is not None:
            parameters_id = self.database_writer.create_entry(
                'parameters',
                parameters_foreign_keys)

        return parameters_id

    # FIXME: YOU ARE HERE. It should be
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
        settings_path = run_settings_run(project_path,
                                         bout_inp_src_dir=None)
        parameter_dict = obtain_project_parameters(settings_path)
        parameter_dict_as_sql_types = \
            cast_parameters_to_sql_type(parameter_dict)
        database.create_parameter_tables(parameter_dict_as_sql_types)
