import pytest
import srsly


@pytest.fixture
def layout():
    return srsly.read_yaml("layouts/2021-layout.yaml")


def test_contains_records(layout):
    assert "BH" in layout
    assert "IR" in layout


def test_bh_definition(layout):
    for variable in layout["BH"]:
        assert "name" in variable
        assert "type" in variable
        assert "start" in variable
        assert "end" in variable
        assert "length" in variable
        assert "drop" in variable
        assert "categorical" in variable
        assert variable["type"] in ["str", "int", "bool", "date"]
        assert variable["start"] >= 0
        assert variable["end"] >= 0
        assert variable["end"] >= variable["start"]
        assert variable["length"] == variable["end"] - variable["start"] + 1
        assert variable["drop"] in [True, False]
        assert variable["categorical"] in [True, False]
        assert (
            variable["categorical"]
            and "values" in variable
            or not variable["categorical"]
            and "values" not in variable
        )


def test_ir_definition(layout):
    for variable in layout["IR"]:
        assert "name" in variable
        assert "type" in variable
        assert "start" in variable
        assert "end" in variable
        assert "length" in variable
        assert "drop" in variable
        assert "categorical" in variable
        assert variable["type"] in ["str", "int", "bool", "date"]
        assert variable["start"] >= 0
        assert variable["end"] >= 0
        assert variable["end"] >= variable["start"]
        assert variable["length"] == variable["end"] - variable["start"] + 1
        assert variable["drop"] in [True, False]
        assert variable["categorical"] in [True, False]
        assert (
            variable["categorical"]
            and "values" in variable
            or not variable["categorical"]
            and "values" not in variable
        )
