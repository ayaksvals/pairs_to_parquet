import duckdb
import os
import copy
import subprocess
import sys
import json
import fire
import time
from itertools import product
import shutil
import warnings

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv


from pairtools.lib import fileio, headerops

from . import duckdb_utils, json_transform, header_metadata



def choose_compressor(method="auto", threads=4):
    """
    Pick the right compression program based on user request and availability.
    If method='none', return an empty command list (no compression).
    """
    compressors = {
        "pigz": ["pigz", "-p", str(threads)],
        "gzip": ["gzip", "-c"],
        "lzop": ["lzop", "-c"],
        "lz4c": ["lz4c", "-c"], 
        "lz4": ["lz4", "-c"],
        "snzip": ["snzip", "-c"],
        "none": []
    }

    if method == "none":
        return "none", compressors["none"]

    if method == "auto":
        # lz4c is blazing fast per core (way faster than gzip), but single-threaded. -> better on smaller chanks
        # pigz is slower per core, but can scale across many threads. -> better on large chunks, like ours
        if shutil.which("pigz"):
            return "pigz", compressors["pigz"]
        elif shutil.which("lz4c"):
            return "lz4c", compressors["lz4c"]
        elif shutil.which("gzip"):
            warnings.warn("lz4c and pigz not found. Falling back to gzip.")
            return "gzip", compressors["gzip"]
        else:
            warnings.warn("No compressor found in PATH. Using no compression.")
            return "none", compressors["none"]
    else:
        if method not in compressors:
            raise ValueError(f"Unsupported compression method: {method}")
        if shutil.which(compressors[method][0]) is None:
            raise RuntimeError(f"Compressor '{method}' not found in PATH.")
        return method, compressors[method]

def write_parquet_iteratable_to_csv(header, iteratable_body, output_path_csv, numb_threads, compress_program="auto"):
    method, cmd = choose_compressor(compress_program, threads=8)

    if not cmd or cmd==[]:
        with open(output_path_csv, "wb") as output_file:
            sink = pa.output_stream(output_file)
            sink.write("".join((line.rstrip() + "\n") for line in header).encode())

            duckdb_utils.write_parquet_to_csv(iteratable_body, sink)

    else:
        with open(output_path_csv, "wb") as output_file, subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=output_file
        ) as proc:
            if proc.stdin is None:
                raise RuntimeError("Failed to open pipe to pigz")

            sink = pa.output_stream(proc.stdin)
            
            sink.write("".join((line.rstrip() + "\n") for line in header).encode()) # header

            duckdb_utils.write_parquet_to_csv(iteratable_body, sink) # body
            
            proc.stdin.close()
            proc.wait()

            if proc.returncode != 0:
                raise RuntimeError(f"{compress_program} compression failed")

def resolve_keys(undefined_keys, column_names):
    """Map user-specified keys (column names or indices) to column names."""

    column_keys=[]
    for col in undefined_keys:
        # check if user listed columns by name or index -> convert to name
        column_keys.append( column_names[col] if col.isnumeric() else col )

    return column_keys
      




# MAIN FUNCTION, which has everything
def duckdb_read_query_write(
    input_path, 
    output_path,
    applied_query: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 16,
    compress_program: str = "pigz",
    UTIL_NAME: str="pairs_to_parquet",
    **kwargs
    ):


    if not(input_path.endswith("pairs.gz") or input_path.endswith("pairs") or input_path.endswith("parquet")):
        raise ValueError(f"Invalid file: {input_path}. Expected a '.pairs.gz'/.pairs/.parquet file.")

    if not(output_path.endswith("pairs.gz") or output_path.endswith("pairs") or output_path.endswith("parquet")):
        raise ValueError(f"Invalid file: {output_path}. Expected a '.pairs.gz'/.pairs/.parquet file.")

    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)
    
    if input_path.endswith("pairs.gz") or input_path.endswith("pairs"):
        instream = fileio.auto_open(
            input_path,
            mode="r",
            nproc=kwargs.get("nproc_in", 1),
            command=kwargs.get("cmd_in", None),
        )

        old_header, body_stream = headerops.get_header(instream)
        new_header = headerops.append_new_pg(old_header, ID=UTIL_NAME, PN=UTIL_NAME)

        header_length = len(old_header)

        column_names = headerops.extract_column_names(new_header)
        column_types = duckdb_utils.classify_column_types_by_name(column_names)

        chromsizes = headerops.extract_chromsizes(new_header)
        unknown_chrom=tuple("!")
        chromosom_field = unknown_chrom+header_metadata.extract_sorted_chromosome_field(chromsizes)

        con = duckdb_utils.setup_duckdb_types(con, chromosom_field)

        query=f"""
        SELECT *
            FROM read_csv('{input_path}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
        """

        if instream != sys.stdin:
            instream.close()


    if input_path.endswith("parquet"):
        old_header=duckdb_utils.duckdb_kv_metadata_to_header(input_path, con)
        new_header = headerops.append_new_pg(old_header, ID=UTIL_NAME, PN=UTIL_NAME)

        query=f"""
        SELECT *
            FROM read_parquet('{input_path}') 
        """
        
    if applied_query!=None:
        query=query+applied_query
        

    if output_path.endswith("gz") or output_path.endswith("pairs"):
        iterator=duckdb_utils.duckdb_query_iterator(con, query)
        write_parquet_iteratable_to_csv(new_header, iterator, output_path, numb_threads, compress_program)

    if output_path.endswith("parquet"):
        kv_metadata = duckdb_utils.header_to_kv_metadata(new_header)
        query = f""" COPY ( {query} ) TO '{output_path}' (FORMAT PARQUET, KV_METADATA {kv_metadata});"""
        con.execute(query)


if __name__ == "__main__":
        fire.Fire()