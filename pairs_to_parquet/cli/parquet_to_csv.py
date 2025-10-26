#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import sys
import click


from pairtools.lib import fileio, pairsam_format, headerops

from ..lib import  duckdb_utils, json_transform, csv_parquet_converter
from . import cli, common_io_options




@cli.command()
@click.argument("input_path", type=str, required=False)
@click.option(
    "-o",
    "--output",
    type=str,
    default="",
    help="output pairs or parquet file."
    " If the path ends with .gz or .lz4, the output is compressed by bgzip "
    "or lz4, correspondingly.",
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
def parquet_to_csv(
    input_path,
    output,
    nproc,
    tmpdir,
    memory,
    compress_program,
    **kwargs,
):
    """Convert  /.parquet   to    /.pairs.gz  file format
    """
    parquet_to_csv_py(
        input_path,
        output,
        nproc,
        tmpdir,
        memory,
        compress_program,
        **kwargs,
    )



def parquet_to_csv_py(input_path,
    output_path,
    nproc,
    tmpdir,
    memory,
    compress_program,
    **kwargs):

    query=None

    csv_parquet_converter.duckdb_read_query_write(input_path, output_path, query, tmpdir, memory, numb_threads=nproc, compress_program=compress_program, UTIL_NAME="pairs_to_parquet_parquet_to_csv")
    
if __name__ == "__main__":
    parquet_to_csv()