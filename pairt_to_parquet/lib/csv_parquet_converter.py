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
from typing import Optional


from . import duckdb_utils, fileio, json_transform, pairsam_format, headerops


UTIL_NAME = "csv_parquet_converter"





# testing
def validate_output_path(output_path):
    if os.path.isdir(output_path):
        raise ValueError(f"Error: {output_path} is a directory, expected a file path.")

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        print(f"Warning: {output_path} already exists and will be overwritten.")



# cant move it to duckdb_utils due to Circular import issue with headerops
def header_to_kv_metadata(header):
    field_names = headerops.extract_field_names(header)
    header_json_dict = json_transform.header_to_json_dict(header, field_names)
    kv_metadata = json_transform.json_dict_to_json_str(header_json_dict)
    return kv_metadata

# cant move it to duckdb_utils due to Circular import issue with headerops
def duckdb_kv_metadata_to_header(parquet_input_path, con=None):
    metadata = duckdb_utils.extract_duckdb_metadata(parquet_input_path, con)
    metadata_dict = duckdb_utils.decode_parquet_metadata_duckdb_as_dict(metadata)
    header = headerops.metadata_dict_to_header_list(metadata_dict) 
    return header


def choose_compressor(method="auto", threads=4):
    """
    Pick the right compression program based on user request and availability.
    """
    compressors = {
        "pigz": ["pigz", "-p", str(threads)],
        "gzip": ["gzip", "-c"],
        "lzop": ["lzop", "-c"],
        "lz4c": ["lz4c", "-c"], 
        "lz4": ["lz4", "-c"],
        "snzip": ["snzip", "-c"],
    }

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
            raise RuntimeError("No supported compressor found in PATH.")
    else:
        if method not in compressors:
            raise ValueError(f"Unsupported compression method: {method}")
        if shutil.which(compressors[method][0]) is None:
            raise RuntimeError(f"Compressor '{method}' not found in PATH.")
        return method, compressors[method]


def write_parquet_iteratable_to_csv(header, iteratable_body, output_path_csv, numb_threads, compress_program="auto"):
    method, cmd = choose_compressor(compress_program, threads=8)

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
      



# CONVERTERS: 

# 1. csv -> parquet
def csv_parquet_converter(
    input_path_csv: str,
    output_path_parquet: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 4,
    **kwargs
): 
    
    # Generate output file path
    instream = fileio.auto_open(
        input_path_csv,
        mode="r",
        nproc=kwargs.get("nproc_in", 1),
        command=kwargs.get("cmd_in", None),
    )
    validate_output_path(output_path_parquet)

    # Extract and modify header
    header, body_stream = headerops.get_header(instream)
    new_header = headerops.append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)

    # DuckDB: connection
    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    # DuckDB: query csv
    chromsizes = headerops.extract_chromsizes(new_header)
    chromosom_field = headerops.extract_chromosome_field(chromsizes)
    con = duckdb_utils.setup_duckdb_types(con, chromosom_field)

    column_names = headerops.extract_column_names(new_header)
    column_types = duckdb_utils.classify_column_types_by_name(column_names)

    header_length = len(new_header)

    # DuckDB: metadata
    kv_metadata = header_to_kv_metadata(new_header)

    # DuckDB: Query
    query = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_path_csv}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
        ) TO '{output_path_parquet}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(query)

    if instream != sys.stdin:
        instream.close()

# 2. parquet + query -> csv.gz:
def parquet_applied_query_csv_gz(
    parquet_input_path: str,
    output_path_csv: str,
    query: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 16
):
    # KV METADATA -> HEADER
    header=duckdb_kv_metadata_to_header(parquet_input_path)

    # DUCKDB SETUP
    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    # ITERATE THROUGH QUERY RESULT
    iterator=duckdb_utils.duckdb_query_iterator(con, query)
    
    write_parquet_iteratable_to_csv(header, iterator, output_path_csv, numb_threads)

# 3. parquet -> csv
def parquet_to_csv_gz_converter(
    parquet_input_path: str,
    output_path: str,
    threads: int = 4
):
    # HEADER
    header=duckdb_kv_metadata_to_header(parquet_input_path)

    # ITERATOR
    iterator=duckdb_utils.parquet_file_iterator(parquet_input_path)

    write_parquet_iteratable_to_csv(header, iterator, output_path_csv, numb_threads)




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
    **kwargs
    ):

    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)
    
    if input_path.endswith("gz") or input_path.endswith("pairs"):
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
        chromosom_field = headerops.extract_chromosome_field(chromsizes)

        con = duckdb_utils.setup_duckdb_types(con, chromosom_field)

        query=f"""
        SELECT *
            FROM read_csv('{input_path}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
        """

        if instream != sys.stdin:
            instream.close()


    if input_path.endswith("parquet") or  input_path.endswith("pq"):
        old_header=duckdb_kv_metadata_to_header(input_path, con)
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

    if output_path.endswith("parquet") or  output_path.endswith("pq"):
        kv_metadata = header_to_kv_metadata(new_header)
        query = f""" COPY ( {query} ) TO '{output_path}' (FORMAT PARQUET, KV_METADATA {kv_metadata});"""
        con.execute(query)


def sort_duckdb(input_path,
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

    column_names = headerops.extract_column_names(header)
    user_columns_to_sort = [c1, c2, p1, p2, pt] + list(extra_col)
    sort_keys=resolve_keys(user_columns_to_sort, column_names)
    query=duckdb_utils.sort_query(sort_keys)

    duckdb_read_query_write(input_path, output_path, query, tmpdir, memory, numb_threads=nproc, compress_program=compress_program)
    




# TYPE1 -> SORT -> TYPE2

# 1. csv-sort-parquet
def csv_sort_parquet(
    input_path: str,
    output_path: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 16,
    **kwargs
):
    # Setup DuckDB connection
    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    # Generate input stream and output file path
    instream = fileio.auto_open(
        input_path,
        mode="r",
        nproc=kwargs.get("nproc_in", 1),
        command=kwargs.get("cmd_in", None),
    )
    validate_output_path(output_path)

    # Extract and modify header
    old_header, body_stream = headerops.get_header(instream)
    new_header = headerops.append_new_pg(old_header, ID=UTIL_NAME, PN=UTIL_NAME)

    # DuckDB: metadata
    kv_metadata = header_to_kv_metadata(new_header)
    
    # DuckDB: ENUM types
    chromsizes = headerops.extract_chromsizes(new_header)
    chromosom_field = headerops.extract_chromosome_field(chromsizes)
    header_length = len(old_header)
    column_names = headerops.extract_column_names(new_header)

    con = duckdb_utils.setup_duckdb_types(con, chromosom_field)
    column_types = duckdb_utils.classify_column_types_by_name(column_names)

    # DuckDB: Query
    query = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_path}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
            ORDER BY chrom1, chrom2, pos1, pos2
        ) TO '{output_path}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(query)

    if instream != sys.stdin:
        instream.close()

# 2. parquet-sort-parquet
def parquet_sort_parquet(
    input_path_parquet: str,
    output_path_parquet: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 16,
):
    validate_output_path(output_path_parquet)

    # HEADER
    header = duckdb_kv_metadata_to_header(input_path_parquet)
    new_header = headerops.append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)
    
    # DuckDB: metadata
    kv_metadata=header_to_kv_metadata(new_header)
    
    # DuckDB: ENUM types
    chromsizes = headerops.extract_chromsizes(new_header)
    chromosom_field = headerops.extract_chromosome_field(chromsizes)
    
    # Setup DuckDB connection
    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)
    con = duckdb_utils.setup_duckdb_types(con, chromosom_field)
    
    # DuckDB: Query
    query = f"""
        COPY (
            SELECT *
            FROM read_parquet('{input_path_parquet}')
            ORDER BY chrom1, chrom2, pos1, pos2
        ) TO '{output_path_parquet}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(query)

# 3. csv-sort-csv
# maybe to use the  existing version of unix sort of pairtools? Or I need to create using duckdb?

# 4. parquet-sort-csv
def parquet_sort_csv(
    input_path_parquet: str,
    output_path_csv: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    numb_threads: int = 16,
):

    # CHANGES REQUIRED! just total mess here

    # Setup DuckDB connection
    con = duckdb_utils.setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    validate_output_path(output_path_csv)

    # Extract and modify header
    metadata = duckdb_utils.extract_duckdb_metadata(input_path_parquet)
    metadata_dict=duckdb_utils.decode_parquet_metadata_duckdb_as_dict(metadata)

    header=headerops.metadata_dict_to_header_list(metadata_dict) 
    header_pg = headerops.append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)
    new_header = header_pg
    
    # DuckDB: metadata
    field_names = headerops.extract_field_names(new_header)
    header_json_dict = json_transform.header_to_json_dict(new_header, field_names)
    kv_metadata = json_transform.json_dict_to_json_str(header_json_dict)
    
    # DuckDB: ENUM types
    chromsizes = headerops.extract_chromsizes(new_header) 
    chromosom_field = headerops.extract_chromosome_field(chromsizes)
    header_length = len(new_header)
    column_names = headerops.extract_column_names(new_header)

    con = duckdb_utils.setup_duckdb_types(con, chromosom_field)
    column_types = duckdb_utils.classify_column_types_by_name(column_names)

    # DuckDB: Query
    code = f"""
        COPY (
            SELECT *
            FROM read_parquet('{input_path_parquet}')
            ORDER BY chrom1, chrom2, pos1, pos2
        ) TO '{output_path_csv}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(code)

# quick tests

# just for testing
def get_output_file_path(input_file, output_dir, new_format='parquet'):
    """
    Generates the output file path in the specified output directory with the same name as the input file.

    Parameters:
        input_file (str): The path to the input file.
        output_dir (str): The path to the output directory.
        new_format (str): The new file format (e.g., 'parquet', 'csv', 'txt').

    Returns:
        str: The full path to the output file in the output directory.
    """
    # Extract the base name of the input file (without directory)
    input_file_name = os.path.basename(input_file)
    
    # Remove the original file extension
    base_name = os.path.splitext(input_file_name)[0]
    
    # Construct the new file name with the specified format
    new_file_name = f"{base_name}.{new_format}"
    
    # Construct the output file path
    output_file_path = os.path.join(output_dir, new_file_name)
    
    return output_file_path



def test_read_query_write(input_type, output_type, **kwargs):
    """
        ('gz', 'gz')
        ('parquet', 'parquet')
        ('gz', 'parquet')
        ('parquet', 'gz')
    """
    small_file=True

    print("Testing: "+input_type+" with "+output_type+":")

    if input_type=="gz":
        if small_file:
            input_file = '/groups/goloborodko/seqdata/hsiehTijan2021/mm10/distiller_0.3.3/results/pairs_library/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.gz' #2.7G
        else:
            input_file = '/users/slavska.olesia/projects/lesia/big_file/original/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.gz' #7.7G
    else:
        if small_file:
            input_file = '/users/slavska.olesia/projects/lesia/small_file/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.parquet' #2.7G
        else:
            input_file = '/users/slavska.olesia/projects/lesia/big_file/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.parquet' #7.7G

    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder, output_type)
    print(f"Output_file: {output_file}")

    temp_dir='/users/slavska.olesia/scratch/slavska.olesia'

    memory_limit='35GB'

    start=time.time()

    query = duckdb_utils.sort_query(["chrom1", "chrom2", "pos1", "pos2"])

    duckdb_read_query_write(
        input_file, 
        output_file,
        applied_query=query,
        temp_directory=temp_dir,
        memory_limit=memory_limit,
        numb_threads=4
    )

    end=time.time()
    print(f"Execution time: {(end - start)/60:.4f} minutes")

def test_all_variants_rqw():
    test_read_query_write('gz', 'gz')
    test_read_query_write('parquet', 'parquet')
    test_read_query_write('gz', 'parquet')
    test_read_query_write('parquet', 'gz')


if __name__ == "__main__":
        fire.Fire()