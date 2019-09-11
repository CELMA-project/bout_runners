from bout_runners.runners.base_runner import single_run

# FIXME: A program should only be made once...could be an idea to
#        copy conduction to a tmp file in order to test make, and let
#        the rest be made through a common fixture

# FIXME: Make a session fixture for making of conduction

# autouse fixture: it will be instantiated before other fixtures within the same scope
# https://docs.pytest.org/en/latest/fixture.html#order-higher-scoped-fixtures-are-instantiated-first

# FIXME: YOU ARE HERE: Make a session fixture using code from test_make
#        Copy test_make to a tmp dir (which is deleted once done)
single_run(path, nproc=1, options='nout=0')
