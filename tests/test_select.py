# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import pytest
import pyarrow.parquet as pq
from pairtools.lib import pairsam_format

from pairs_to_parquet.lib.duckdb_utils import duckdb_kv_metadata_to_header

testdir = os.path.dirname(os.path.realpath(__file__))
mock_parquet_path = os.path.join(testdir, "data", "mock.parquet")
mock_chromsizes_path = os.path.join(testdir, "data", "mock.chrom.sizes")


def read_parquet_as_lines(path):
    table = pq.read_table(path)

    # Convert to batch of Python rows (list of tuples)
    columns = table.column_names
    rows = table.to_pylist()  # list of dicts

    # Convert each row dict -> tab-separated string
    lines = [
        "\t".join("" if r[col] is None else str(r[col]) for col in columns)
        for r in rows
    ]

    body = [
        l.strip()
        for l in lines
        if not l.startswith("#") and l.strip()
    ]

    return body






def test_preserve():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_preserve_mock.parquet")
    try:
        result = subprocess.check_output(
            ["python", "-m", "pairs_to_parquet", "select", "True", mock_parquet_path, "-o", mock_output_parquet_path],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)

    assert all(l in original_body for l in output_body)

def test_equal():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_pairTypeEqual_mock.parquet")
    try:
        result = subprocess.check_output(
            [
                "python", "-m",
                "pairs_to_parquet",
                "select",
                '(pair_type == "RU") or (pair_type == "UR") or (pair_type == "UU")',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    
    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)

    assert all(l.split("\t")[7] in ["RU", "UR", "UU"] for l in output_body)
    assert all(
        l in output_body for l in original_body if l.split("\t")[7] in ["RU", "UR", "UU"]
    )

def test_csv():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_csvMatch_mock.parquet")
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                'csv_match(pair_type, "RU,UR,UU")',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)

    assert all(l.split("\t")[7] in ["RU", "UR", "UU"] for l in output_body)
    assert all(
        l in output_body for l in original_body if l.split("\t")[7] in ["RU", "UR", "UU"]
    )

def test_wildcard():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_wildcard_mock.parquet")
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                'wildcard_match(pair_type, "*U")',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)

    assert all(l.split("\t")[7] in ["NU", "MU", "RU", "UU"] for l in output_body)
    assert all(
        l in output_body
        for l in original_body
        if l.split("\t")[7] in ["NU", "MU", "RU", "UU"]
    )

def test_regex():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_regex_mock.parquet")
    
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                'regex_match(pair_type, "[NM]U")',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)


    assert all(l.split("\t")[7] in ["NU", "MU"] for l in output_body)
    assert all(
        l in output_body for l in original_body if l.split("\t")[7] in ["NU", "MU"]
    )

def test_chrom_subset():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_chromSubset_mock.parquet")
    
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                "True",
                "--chrom-subset",
                mock_chromsizes_path,
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)
    output_header = duckdb_kv_metadata_to_header(mock_output_parquet_path)

    chroms_from_chrom_field = [
        l.strip().split()[1:]
        for l in output_header
        if l.startswith("#chromosomes:")
    ][0]

    assert set(chroms_from_chrom_field) == set(["chr1", "chr2"])

    chroms_from_chrom_sizes = [
        l.strip().split()[1] for l in output_header if l.startswith("#chromsize:")
    ]

    assert set(chroms_from_chrom_sizes) == set(["chr1", "chr2"])

def test_remove_columns():
    """Test removal of columns from the file
    Example run:
    pairs_to_parquet select True --remove-columns sam1,sam2 tests/data/mock.parquet
    """
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_reove_columns_mock.parquet")
    
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                "True",
                "--remove-columns",
                "sam1,sam2",
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    # check if the columns are removed properly:
    output_header = duckdb_kv_metadata_to_header(mock_output_parquet_path)
    # pairsam_header = [l.strip() for l in result.split("\n") if l.startswith("#")]
    output_body=read_parquet_as_lines(mock_output_parquet_path)

    for l in output_header:
        if l.startswith("#columns:"):
            line = l.strip()
            assert (
                line
                == "#columns: readID chrom1 pos1 chrom2 pos2 strand1 strand2 pair_type"
            )

    # check that the pairs got assigned properly
    for l in output_body:
        if l.startswith("#") or not l:
            continue

        assert len(l.split(pairsam_format.PAIRSAM_SEP)) == 8

def test_region_match():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_regionMatch_mock.parquet")
    
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                'region_match(chrom1, pos1, "chr1", 0, 50)',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)
    output_header = duckdb_kv_metadata_to_header(mock_output_parquet_path)

    # Verify all output rows have chrom1="chr1" and pos1 within range
    for l in output_body:
        fields = l.split("\t")
        chrom1, pos1 = fields[1], int(fields[2])
        assert chrom1 == "chr1"
        assert 0 <= pos1 <= 50

    # Verify all matching rows from input are in output
    for l in original_body:
        fields = l.split("\t")
        chrom1, pos1 = fields[1], int(fields[2])
        if chrom1 == "chr1" and 0 <= pos1 <= 50:
            assert l in output_body

def test_region_match_no_end():
    mock_output_parquet_path = os.path.join(testdir, "data/select_results", "select_region_match_no_end_mock.parquet")
    
    try:
        result = subprocess.check_output(
            [
                "python",
                "-m",
                "pairs_to_parquet",
                "select",
                'region_match(chrom1, pos1, "chr1", 50)',
                mock_parquet_path,
                "-o", mock_output_parquet_path,
            ],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())
        raise e

    original_body=read_parquet_as_lines(mock_parquet_path)
    output_body=read_parquet_as_lines(mock_output_parquet_path)
    output_header = duckdb_kv_metadata_to_header(mock_output_parquet_path)


    # Verify all output rows have chrom1="chr1" and pos1 >= 50
    for l in output_body:
        fields = l.split("	")
        chrom1, pos1 = fields[1], int(fields[2])
        assert chrom1 == "chr1"
        assert pos1 >= 50

    # Verify all matching rows from input are in output
    for l in original_body:
        fields = l.split("	")
        chrom1, pos1 = fields[1], int(fields[2])
        if chrom1 == "chr1" and pos1 >= 100:
            assert l in output_body