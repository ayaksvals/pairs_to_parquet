import json
import pytest
from unittest.mock import patch

from pairtools.lib import headerops
from pairs_to_parquet.lib.json_transform import (
    header_to_json_dict,
    json_dict_to_json_str,
    decode_and_parse_json,
)


# ---- MOCKING headerops.extract_fields ----
@pytest.fixture
def mock_extract_fields(monkeypatch):
    def mock_func(header_rest, field_name, _):  # we ignore that extract fields returns header_rest, it is not that important
        mock_data = {
            "columns": (["readID pairID chrom1 chrom2 pos1 pos2 strand1 strand2"], ""),
            "chromsize": (["chr1 1000", "chr2 2000"], ""),
            "samheader": (["@SQ SN:chr1 LN:1000", "@SQ SN:chr2 LN:2000"], ""),
            "sorted": (["chr1-chr2-pos1-pos2"], ""),
            "shape": (["triangular"], ""),
            "genome_assembly": (["hg38"], ""),
        }
        return mock_data.get(field_name, ([], ""))
    monkeypatch.setattr(headerops, "extract_fields", mock_func)



# -------------------------------
# TEST header_to_json_dict
# -------------------------------
def test_header_to_json_dict_basic(mock_extract_fields):
    header = "" #does not matter, according to mocking of extract fields
    field_names = ["columns", "chromsize", "samheader", "sorted", "shape", "genome_assembly"]

    result = header_to_json_dict(header, field_names)

    # Validate structure
    assert isinstance(result, dict)
    assert "columns" in result
    assert json.loads(result["columns"]) == ["readID", "pairID", "chrom1", "chrom2", "pos1", "pos2", "strand1", "strand2"]

    # chromsize should be parsed into dict of ints
    chroms = json.loads(result["chromsize"])
    assert chroms == {"chr1": 1000, "chr2": 2000}

    # simple values
    assert json.loads(result["sorted"]) == "chr1-chr2-pos1-pos2"
    assert json.loads(result["shape"]) == "triangular"
    assert json.loads(result["genome_assembly"]) == "hg38"


def test_header_to_json_dict_with_extra_rest(monkeypatch):
    """If header_rest remains, it should add 'format' key."""
    def mock_func(header_rest, field_name, _):
        return (["format"], ["## pairs format v1.0.0"])
    monkeypatch.setattr(headerops, "extract_fields", mock_func)

    result = header_to_json_dict("## header example", ["columns"])
    assert "format" in result
    assert json.loads(result["format"]) == "## pairs format v1.0.0"


# -------------------------------
# TEST json_dict_to_json_str
# -------------------------------
def test_json_dict_to_json_str_normalization():
    header_json_dict = {
        "# columns:": '{"a": 1}',
        "sam header": '{"b": 2}',
        "shape:": '"triangular"'
    }
    result = json_dict_to_json_str(header_json_dict)
    assert result == {
        "columns": '{"a": 1}',
        "samheader": '{"b": 2}',
        "shape": '"triangular"'
    }



# -------------------------------
# TEST decode_and_parse_json
# -------------------------------
@pytest.mark.parametrize(
    "input_str,expected",
    [
        ('"simple string"', "simple string"),
        ('["a", "b", "c"]', ["a", "b", "c"]),
        ('{"key": "value"}', {"key": "value"}),
    ]
)
def test_decode_and_parse_json_valid(input_str, expected):
    assert decode_and_parse_json(input_str) == expected


def test_decode_and_parse_json_with_unicode_escape():
    s = '"hello \\u0041"'  # \u0041 = 'A'
    result = decode_and_parse_json(s)
    assert result == "hello A"


def test_decode_and_parse_json_invalid_json(caplog):
    bad_json = '"unterminated string'
    result = decode_and_parse_json(bad_json)
    assert "JSONDecodeError" in result
    assert any("JSONDecodeError" in msg for msg in caplog.text.splitlines())


def test_decode_and_parse_json_unicode_error():
    bad_value = "\\xZZ"  # invalid hex escape sequence for unicode_escape
    result = decode_and_parse_json(bad_value)
    assert "Unicode decode error" in result
    assert "original value" in result

