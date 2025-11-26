# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import pytest

testdir = os.path.dirname(os.path.realpath(__file__)) # __file__ is a built-in variable that Python automatically sets when it loads a module or script from a file.


def test_mock_pairs():
    mock_pairs_path = os.path.join(testdir, "data", "mock.pairs")
    mock_output_pairs_path = os.path.join(testdir, "data/sort_results", "sorted_mock.pairs")
    try:
        result = subprocess.check_output(
            ["python", "-m", "pairs_to_parquet", "sort", "-o", mock_output_pairs_path, "--compress-program", "none", mock_pairs_path],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())

        raise e

    # Check that the only changes strings are a @PG record of a SAM header,
    # the "#sorted" entry and chromosomes
    pairs_header = [
        l.strip() for l in open(mock_pairs_path, "r") if l.startswith("#")
    ]
    output_header = [l.strip() for l in open(mock_output_pairs_path, "r") if l.startswith("#")]

    print(output_header)
    print(pairs_header)
    for l in output_header:
        if not any([l in l2 for l2 in pairs_header]):
            assert (
                l.startswith("#samheader: @PG")
                or l.startswith("#sorted")
                or l.startswith("#chromosomes")
            )

    pairs_body = [
        l.strip()
        for l in open(mock_pairs_path, "r")
        if not l.startswith("#") and l.strip()
    ]
    output_body = [
        l.strip()
        for l in open(mock_output_pairs_path, "r")
        if not l.startswith("#") and l.strip()
    ]

    # check that all pairs entries survived sorting:
    assert len(pairs_body) == len(output_body)

    # check the sorting order of the output:
    prev_pair = None
    for l in output_body:
        cur_pair = l.split("\t")[1:8]
        if prev_pair is not None:
            assert cur_pair[0] >= prev_pair[0]
            if cur_pair[0] == prev_pair[0]:
                assert cur_pair[2] >= prev_pair[2]
                if cur_pair[2] == prev_pair[2]:
                    assert int(cur_pair[1]) >= int(prev_pair[1])
                    if int(cur_pair[1]) == int(prev_pair[1]):
                        assert int(cur_pair[3]) >= int(prev_pair[3])

        prev_pair = cur_pair






def test_mock_pairs_parquet():
    ######## Check later for parquet, right now used as creating sorted_mock.parquet:
    
    mock_pairs_path = os.path.join(testdir, "data", "mock.pairs")
    mock_output_pairs_path = os.path.join(testdir, "data/sort_results", "sorted_mock.parquet")
    try:
        result = subprocess.check_output(
            ["python", "-m", "pairs_to_parquet", "sort", "-o", mock_output_pairs_path, "--compress-program", "none", mock_pairs_path],
        )
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(sys.exc_info())

        raise e


