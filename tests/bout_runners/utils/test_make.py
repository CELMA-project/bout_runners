import os
from pathlib import Path
from bout_runners.utils.make import MakeProject
from dotenv import load_dotenv


# FIXME: YOU ARE HERE: ADD BOUT-DEV PATH TO DOTENV
#        THIS CAN BE USED IN MAKEFILE OF CELMA, BUT MORE IMPORTANTLY
#        TO FIND WHERE THE EXAMPLES ARE STORED IN THIS TEST (AND TO
#        OVERWRITE IT WITH CLI FOR TRAVIS)


def test_make_project():
    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH'))
    MakeProject()
    print(bout_path)