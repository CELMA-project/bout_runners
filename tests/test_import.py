import unittest
from tests.conftest import *  # Add the python path
from boututils.datafile import DataFile


class TestImport(unittest.TestCase):
    def test_import(self):
        d = DataFile()
        self.assertIsInstance(d, DataFile)


if __name__ == '__main__':
    unittest.main()
