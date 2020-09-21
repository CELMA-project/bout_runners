"""Contains functions for serializing data."""

import json
from typing import Any


def is_jsonable(data: Any) -> bool:
    """
    Check whether the data is possible to serialize.

    Parameters
    ----------
    data : object
        Data to serialize

    Returns
    -------
    bool
        True if it is possible to serialize the data
    """
    try:
        json.dumps(data)
        return True
    except (TypeError, OverflowError):
        return False
