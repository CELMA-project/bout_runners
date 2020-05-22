"""Contains unittests for run parameters."""


from bout_runners.parameters.run_parameters import RunParameters


def test_run_parameters() -> None:
    """Test that the RunParameters is setting the parameters."""
    run_parameters = RunParameters({"global": {"append": False}, "mesh": {"nx": 4}})
    expected_str = "append=False mesh.nx=4 "
    assert run_parameters.run_parameters_str == expected_str
