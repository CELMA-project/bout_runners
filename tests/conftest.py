"""Global fixtures for the test routines."""

pytest_plugins = (
    "tests.fixtures.build",
    "tests.fixtures.database",
    "tests.fixtures.file_operations",
    "tests.fixtures.log",
    "tests.fixtures.metadata",
    "tests.fixtures.monkey_patch",
    "tests.fixtures.parameters",
    "tests.fixtures.paths",
    "tests.fixtures.run_preparation",
)
