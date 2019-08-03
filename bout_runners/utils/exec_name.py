from pathlib import Path
from bout_runners.utils.read_makefile import BoutMakefileVariable
from bout_runners.utils.read_makefile import ReadMakefileError


# FIXME: infer Makefile name

def get_exec_name(makefile_root_path, makefile_name=None):
    """
    Gets the name of the project executable

    This method first searches for the 'TARGET' variable in the
    makefile. If not found it infers the name from the 'SOURCEC'
    variable.

    Parameters
    ----------
    makefile_root_path : Path or str
        Root path of make file
    makefile_name : None or str
        The name of the makefile.
        If set to None, it tries the following names, in order:
        'GNUmakefile', 'makefile' and 'Makefile'

    Returns
    -------
    exec_name : str
        Name of the executable
    """

    if makefile_name is None:
        makefile_name = infer_makefile_name(makefile_root_path)

    makefile_path = Path(makefile_root_path).joinpath(makefile_name)

    try:
        exec_name = BoutMakefileVariable('TARGET', makefile_path)\
            .get_variable_value()
    except ReadMakefileError:
        exec_name = BoutMakefileVariable('SOURCEC', makefile_path)\
            .get_variable_value()
        # Strip the name from the last .c*
        split_by = '.c'
        split_list = exec_name.split(split_by)
        split_to_join = \
            split_list if len(split_list) == 1 else split_list[:-1]
        exec_name = f'{split_by}'.join(split_to_join)

    return exec_name


def infer_makefile_name(makefile_root_path):
    """
    Searches for a valid Makefile

    The order of the search is 'GNUmakefile', 'makefile' and 'Makefile'

    Parameters
    ----------
    makefile_root_path

    Returns
    -------
    makefile_name : str
        The name of the Makefile
    """

    possible_names = ('GNUmakefile',
                      'Makefile',
                      'makefile')

    makefile_name = None

    for name in possible_names:
        if makefile_root_path.joinpath(name):
            makefile_name = name
            break

    if makefile_name is None:
        msg = f'Could not find a valid Makefile name in ' \
              f'{makefile_root_path}. Valid Makefile names are ' \
              f'{" ,".join(possible_names)}'
        raise FileNotFoundError(msg)

    return makefile_name
