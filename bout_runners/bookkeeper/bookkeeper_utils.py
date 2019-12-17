"""Module containing utils only used in the bookkeeper package."""


import ast
import configparser
import platform
import re


def obtain_project_parameters(settings_path):
    """
    Return the project parameters from the settings file.

    Parameters
    ----------
    settings_path : Path
        Path to the settings file

    Returns
    -------
    parameter_dict : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value_type'}}

    Notes
    -----
    1. The section less part of BOUT.settings will be renamed `global`
    2. In the `global` section, the keys `d` and the directory to the
       BOUT.inp file will be removed
    3. If the section `all` is present in BOUT.settings, the section
       will be renamed `all_boundaries` as `all` is a protected SQL
       keyword
    4. The section `run` will be dropped due to bout_runners own
       `run` table
    """
    type_map = {'bool': 'INTEGER',  # No bool type in SQLite
                'float': 'REAL',
                'int': 'INTEGER',
                'str': 'TEXT'}

    # The settings file lacks a header for the global parameter
    # Therefore, we add add the header [global]
    with settings_path.open('r') as settings_file:
        settings_memory = f'[global]\n{settings_file.read()}'

    config = configparser.ConfigParser()
    config.read_string(settings_memory)

    parameter_dict = dict()

    for section in config.sections():
        parameter_dict[section] = dict()
        for key, val in config[section].items():
            # Strip comments
            capture_all_but_comment = '^([^#]*)'
            matches = re.findall(capture_all_but_comment, val, re.M)

            # Exclude comment line
            if len(matches) == 0:
                continue

            # Capitalize in case of boolean
            stripped_val = matches[0].capitalize()

            # If type is not found, type is str
            try:
                val_type = type(ast.literal_eval(stripped_val))
            except (SyntaxError, ValueError):
                val_type = str

            parameter_dict[section][key] = type_map[val_type.__name__]

    # NOTE: Bug in .settings: -d path is captured with # not in use
    bout_inp_dir = settings_path.parent
    parameter_dict['global'].pop('d', None)
    parameter_dict['global'].pop(str(bout_inp_dir).lower(), None)

    if 'all' in parameter_dict.keys():
        parameter_dict['all_boundaries'] = parameter_dict.pop('all')

    # Drop run as bout_runners will make its own table with that name
    parameter_dict.pop('run')

    return parameter_dict


def get_system_info_as_sql_type():
    """
    Return the system information.

    Returns
    -------
    sys_info_dict : dict
        Dictionary with the attributes of the system as keys and the
        type as values
    """
    sys_info = platform.uname()

    # Get member data, see
    # https://stackoverflow.com/questions/11637293/iterate-over-object-attributes-in-python
    # for details
    attributes = tuple(name for name in dir(sys_info)
                       if not name.startswith('_') and not
                       callable(getattr(sys_info, name)))

    sys_info_dict = {att: 'TEXT' for att in attributes}

    return sys_info_dict


def get_create_table_statement(table_name,
                               columns=None,
                               primary_key='id',
                               foreign_keys=None):
    """
    Return a SQL string which can be used to create the table.

    Parameters
    ----------
    table_name : str
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
    create_statement = f'CREATE TABLE {table_name} \n('

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
        for name, (f_table_name, key_in_table) in foreign_keys.items():
            create_statement += \
                (f'    FOREIGN KEY({name}) \n'
                 f'        REFERENCES {f_table_name}({key_in_table}),'
                 f'\n')

    # Replace last comma with )
    create_statement = f'{create_statement[:-2]})'

    return create_statement
