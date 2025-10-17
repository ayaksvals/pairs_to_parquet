#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import sys
import click
import subprocess
import shutil
import warnings

from ..lib import fileio, pairsam_format, headerops, duckdb_utils, json_transform, csv_parquet_converter
from . import cli, common_io_options




@cli.command()
@click.argument("pairs_path", type=str, required=False)
@click.option(
    "-o",
    "--output",
    type=str,
    default="",
    help="output pairs file."
    " If the path ends with .gz or .lz4, the output is compressed by bgzip "
    "or lz4, correspondingly. By default, the output is printed into stdout.",
)
@click.option(
    "--c1",
    type=str,
    default=pairsam_format.COLUMNS_PAIRS[1],
    help=f"Chrom 1 column; default {pairsam_format.COLUMNS_PAIRS[1]}"
    "[input format option]",
)
@click.option(
    "--c2",
    type=str,
    default=pairsam_format.COLUMNS_PAIRS[3],
    help=f"Chrom 2 column; default {pairsam_format.COLUMNS_PAIRS[3]}"
    "[input format option]",
)
@click.option(
    "--p1",
    type=str,
    default=pairsam_format.COLUMNS_PAIRS[2],
    help=f"Position 1 column; default {pairsam_format.COLUMNS_PAIRS[2]}"
    "[input format option]",
)
@click.option(
    "--p2",
    type=str,
    default=pairsam_format.COLUMNS_PAIRS[4],
    help=f"Position 2 column; default {pairsam_format.COLUMNS_PAIRS[4]}"
    "[input format option]",
)
@click.option(
    "--pt",
    type=str,
    default=pairsam_format.COLUMNS_PAIRS[7],
    help=f"Pair type column; default {pairsam_format.COLUMNS_PAIRS[7]}"
    "[input format option]",
)
@click.option(
    "--extra-col",
    nargs=1,
    type=str,
    multiple=True,
    help="Extra column (name or numerical index) that is also used for sorting."
    "The option can be provided multiple times."
    'Example: --extra-col "phase1" --extra-col "phase2". [output format option]',
)
@click.option(
    "--nproc",
    type=int,
    default=8,
    show_default=True,
    help="Number of processes to split the sorting work between.",
)
@click.option(
    "--tmpdir",
    type=str,
    default="",
    help="Custom temporary folder for sorting intermediates.",
)
@click.option(
    "--memory",
    type=str,
    default="2G",
    show_default=True,
    help="The amount of memory used by default.",
)
@click.option(
    "--compress-program",
    type=str,
    default="auto",
    show_default=True,
    help="A binary to compress temporary sorted chunks. "
    "Must decompress input when the flag -d is provided. "
    "Suggested alternatives: gzip, lzop, lz4c, snzip. "
    'If "auto", then use lz4c if available, and gzip '
    "otherwise.",
)
@common_io_options
def sort_duckdb(
    pairs_path,
    output,
    c1,
    c2,
    p1,
    p2,
    pt,
    extra_col,
    nproc,
    tmpdir,
    memory,
    compress_program,
    **kwargs,
):
    """Sort a .pairs/.pairsam file.

    Sort pairs in the lexicographic order along chrom1 and chrom2, in the
    numeric order along pos1 and pos2 and in the lexicographic order along
    pair_type.

    PAIRS_PATH : input .pairs/.pairsam file. If the path ends with .gz or .lz4, the
    input is decompressed by bgzip or lz4c, correspondingly. By default, the
    input is read as text from stdin.
    """
    sort_duckdb_py(
        pairs_path,
        output,
        c1,
        c2,
        p1,
        p2,
        pt,
        extra_col,
        nproc,
        tmpdir,
        memory,
        compress_program,
        **kwargs,
    )



def sort_duckdb_py(input_path,
    output_path,
    c1,
    c2,
    p1,
    p2,
    pt,
    extra_col,
    nproc,
    tmpdir,
    memory,
    compress_program,
    **kwargs):

    print("sort duckdb")

    if input_path.endswith("gz") or input_path.endswith("pairs"):
        instream = fileio.auto_open(
            input_path,
            mode="r",
            nproc=kwargs.get("nproc_in", 1),
            command=kwargs.get("cmd_in", None),
        )

        header, body_stream = headerops.get_header(instream)
        
        if instream != sys.stdin:
            instream.close()


    if input_path.endswith("parquet") or  input_path.endswith("pq"):
        header=duckdb_kv_metadata_to_header(input_path, con)
        

    column_names = headerops.extract_column_names(header)
    user_columns_to_sort = [c1, c2, p1, p2, pt] + list(extra_col)
    sort_keys=csv_parquet_converter.resolve_keys(user_columns_to_sort, column_names)
    query=duckdb_utils.sort_query(sort_keys)

    csv_parquet_converter.duckdb_read_query_write(input_path, output_path, query, tmpdir, memory, numb_threads=nproc, compress_program=compress_program)
    
if __name__ == "__main__":
    sort_duckdb()