import pytest
from pairs_to_parquet.lib.header_metadata import (
    extract_field_names,
    extract_sorted_chromosome_field,
    metadata_dict_to_header_list,
)


# -------------------------------
# TEST extract_field_names
# -------------------------------
def test_extract_field_names_basic():
    header_lines = [
        "#columns: readID pairID chrom1 chrom2 pos1 pos2",
        "#chromsize: chr1 1000",
        "#sorted: chr1-chr2-pos1-pos2",
        "#shape: triangular",
        "#genome_assembly: hg38",
        "#samheader: @SQ SN:chr1 LN:1000",
    ]
    result = extract_field_names(header_lines)

    # Should extract unique names (without # and : at the end)
    assert set(result) == {"columns", "chromsize", "sorted", "shape", "genome_assembly", "samheader"}


def test_extract_field_names_ignores_empty_and_duplicates():
    header_lines = [
        "#columns: x y z",
        "#columns: again",
        "#:",
        "#chromsize: chr1 1000",
    ]
    result = extract_field_names(header_lines)
    assert "columns" in result
    assert "chromsize" in result
    # Should not add duplicates or empty
    assert "" not in result
    assert len(result) == 2


# -------------------------------
# TEST extract_sorted_chromosome_field
# -------------------------------
def test_extract_sorted_chromosome_field_basic():
    chromsizes = {"chr3": 300, "chr1": 100, "chr2": 200}
    result = extract_sorted_chromosome_field(chromsizes)
    assert result == ("chr1", "chr2", "chr3")


def test_extract_sorted_chromosome_field_empty():
    assert extract_sorted_chromosome_field({}) == ()


# -------------------------------
# TEST metadata_dict_to_header_list
# -------------------------------
def test_metadata_dict_to_header_list_basic():
    metadata_dict = {
        "format": "## pairs format v1.0",
        "sorted": "chr1-chr2-pos1-pos2",
        "shape": "upper triangle",
        "genome_assembly": "hg38",
        "chromsize": {"chr1": 1000, "chr2": 2000},
        "samheader": ["@SQ SN:chr1 LN:1000", "@SQ SN:chr2 LN:2000"],
        "columns": ["readID", "pairID", "chrom1", "chrom2", "pos1", "pos2"],
    }

    header = metadata_dict_to_header_list(metadata_dict)

    # Expected structure
    assert any(line.startswith("#chromsize:") for line in header)
    assert any("#samheader:" in line for line in header)
    assert "#columns: readID pairID chrom1 chrom2 pos1 pos2" in header[-1]

    # Chromsize lines should be formatted correctly
    chrom_lines = [l for l in header if l.startswith("#chromsize:")]
    assert "#chromsize: chr1 1000" in chrom_lines
    assert "#chromsize: chr2 2000" in chrom_lines


def test_metadata_dict_to_header_list_missing_optional_fields():
    # Minimal dict
    metadata_dict = {
        "format": "## pairs format v1.0",
        "sorted": "chr1-chr2-pos1-pos2",
        "shape": "upper triangle",
        "genome_assembly": "mm10",
        "chromsize": {},
        "samheader": [],
        "columns": ["a", "b", "c"],
    }

    header = metadata_dict_to_header_list(metadata_dict)
    assert isinstance(header, list)
    assert "#columns: a b c" in header
    assert "#shape: upper triangle" in header
    assert "#genome_assembly: mm10" in header