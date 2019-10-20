# FIXME: Placeholder for filling in the database
import contextlib
import sqlite3


def create_table(database_path, sql_statement):
    """
    Creates a table in the database

    Parameters
    ----------
    database_path : Path or str
        Path to database
    sql_statement : str
        The query to execute
    """
    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/47501337/2786884
    # Auto-closes connection
    with contextlib.closing(sqlite3.connect(str(database_path))) as con:
        # Auto-commits
        with con as c:
            # Auto-closes cursor
            with contextlib.closing(c.cursor()) as cur:
                # Check if tables are present
                cur.execute(sql_statement)


def get_create_table_statement(name,
                               columns=None,
                               primary_key='id',
                               foreign_keys=None):
    """
    Returns a SQL string which can be used to create

    Parameters
    ----------
    name : str
        Name of the table
    columns : dict or None
        Dictionary where the key is the column name and the value is
        the type
    primary_key : str
        Name of the primary key (the type is set to INTEGER)
    foreign_keys : dict or None
        Dictionary where the key is the column in this table to be
        used as a foreign key and the value is the tuple
        consisting of (name_of_the_table, key_in_table) to refer to

    Returns
    -------
    create_statement : str
        The SQL statement which creates table
    """

    create_statement = f'CREATE TABLE {name} \n('

    create_statement += f'   {primary_key} INTEGER PRIMARY KEY,\n'

    if columns is not None:
        for name, sql_type in columns.items():
            create_statement += f'    {name} {sql_type},\n'

    if foreign_keys is not None:
        # Create the key as column
        # NOTE: All keys are integers
        for name in foreign_keys.keys():
            create_statement += f'    {name} INTEGER,\n'

        # Add constraint
        for name, (table_name, key_in_table) in foreign_keys.items():
            create_statement += \
                (f'    FOREIGN KEY({name}) \n'
                 f'        REFERENCES {table_name}({key_in_table}),\n')

    # Replace last comma with )
    create_statement = f'{create_statement[:-2]})'

    return create_statement


def create_parameter_tables(database_path, parameter_dict):
    """
    Create a table for each section in BOUT.settings and a join table

    Parameters
    ----------
    database_path : Path
        The path to the database
    parameter_dict : dict
        The dictionary on the same form as the output of
        FIXME: Update Path
        bout_runners.obtain_project_parameters

    Notes
    -----
    All `:` will be replaced by `_` in the section names
    """
    parameters_foreign_keys = dict()
    for section in parameter_dict.keys():
        # Replace bad characters for SQL
        section_name = section.replace(':', '_')
        # Generate foreign keys for the parameters table
        parameters_foreign_keys[f'{section_name}_id'] =\
            (section_name, 'id')

        columns = dict()
        for parameter, value_type in parameter_dict[section].items():
            # Generate the columns
            columns[parameter] = value_type

        # Creat the section table
        section_statement = \
            get_create_table_statement(name=section_name,
                                       columns=columns)
        create_table(database_path, section_statement)

    # Create the join table
    parameters_statement = get_create_table_statement(
        name='parameters', foreign_keys=parameters_foreign_keys)
    create_table(database_path, parameters_statement)