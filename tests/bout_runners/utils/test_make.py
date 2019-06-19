from bout_runners.utils.make import MakeProject


# FIXME: YOU ARE HERE: ADD BOUT-DEV PATH TO DOTENV
#        THIS CAN BE USED IN MAKEFILE OF CELMA, BUT MORE IMPORTANTLY
#        TO FIND WHERE THE EXAMPLES ARE STORED IN THIS TEST (AND TO
#        OVERWRITE IT WITH CLI FOR TRAVIS)


def test_make_project():
    MakeProject()