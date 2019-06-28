from pathlib import Path
from bout_runners.utils.read_make_file import ReadBoutMakeFile
from bout_runners.utils.read_make_file import BoutMakeFileVariable


DATA_PATH = Path(__file__).absolute().parents[1].joinpath('data')


def test_read_bout_make_file():
    """
    Tests that ReadBoutMakeFile can read a file
    """

    reader = ReadBoutMakeFile(DATA_PATH.joinpath('test_read'))
    assert reader.content == 'This is some text'