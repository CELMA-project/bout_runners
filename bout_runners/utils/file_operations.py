import inspect
from pathlib import Path


def get_caller_dir():
    """
    Returns the directory of the topmost caller file

    Returns
    -------
    caller_dir : Path
        The path of the topmost caller

    References
    ----------
    [1] https://stackoverflow.com/a/1095621/2786884
    """

    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    caller_dir = Path(module.__file__).parent

    return caller_dir
