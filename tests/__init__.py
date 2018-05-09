import sys
from pathlib import Path


path = Path(__file__).absolute().parents[1].\
    joinpath('BOUT-dev', 'tools', 'pylib')
sys.path.append(str(path))
