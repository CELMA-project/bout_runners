"""Contains unittests for the ProcessorSplit class."""


import pytest
from bout_runners.submitter.processor_split import ProcessorSplit


def test_processor_split():
    """Test that the ProcessorSplit is setting the parameters."""
    processor_split = ProcessorSplit(
        number_of_processors=1, number_of_nodes=1, processors_per_node=1
    )
    assert processor_split.number_of_processors == 1

    with pytest.raises(ValueError):
        processor_split.number_of_processors = 2
