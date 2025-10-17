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


from ..lib import duckdb_utils, fileio, json_transform, pairsam_format, headerops, csv_parquet_converter


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



# TESTS:
def test_csv_parquet_converter(**kwargs):
    small_file=True
    if small_file:
        input_file = '/groups/goloborodko/seqdata/hsiehTijan2021/mm10/distiller_0.3.3/results/pairs_library/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.gz' #2.7G
    else:
        input_file = '/users/slavska.olesia/projects/lesia/big_file/original/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.gz' #7.7G
    
    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder)
    print(f"Output_file: {output_file}")

    temp_dir='/users/slavska.olesia/scratch/slavska.olesia'

    memory_limit='8GB'

    csv_parquet_converter.csv_parquet_converter(input_file, output_file, temp_dir, memory_limit)

def test_parquet_csv_converter(**kwargs):
    small_file=True
    if small_file:
        input_file = '/users/slavska.olesia/projects/lesia/small_file/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.parquet' #2.7G
    else:
        input_file = '/users/slavska.olesia/projects/lesia/big_file/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.parquet' #7.7G
    
    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder, new_format='csv.gz')
    print(f"Output_file: {output_file}")

    csv_parquet_converter.parquet_to_csv_gz_converter(input_file, output_file, threads=4)



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

    csv_parquet_converter.duckdb_read_query_write(
        input_file, 
        output_file,
        applied_query=None,
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





# 5. tests:
def test_csv_sort_parquet(**kwargs):
    small_file=True
    if small_file:
        input_file = '/groups/goloborodko/seqdata/hsiehTijan2021/mm10/distiller_0.3.3/results/pairs_library/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.gz' #2.7G
    else:
        input_file = '/users/slavska.olesia/projects/lesia/big_file/original/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.gz' #7.7G
    
    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder)
    print(f"Output_file: {output_file}")

    temp_dir='/users/slavska.olesia/scratch/slavska.olesia'

    memory_limit='8GB'

    csv_parquet_converter.csv_sort_parquet(input_file, output_file, temp_dir, memory_limit)

def test_parquet_sort_parquet(**kwargs): 
    small_file=True
    if small_file:
        input_file = '/users/slavska.olesia/projects/lesia/small_file/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.parquet' #2.7G
    else:
        input_file = '/users/slavska.olesia/projects/lesia/big_file/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.parquet' #7.7G
    
    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file/pq_sort_pq'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder, new_format='parquet')
    print(f"Output_file: {output_file}")

    temp_dir='/users/slavska.olesia/scratch/slavska.olesia'

    memory_limit='200GB'
    start=time.time()
    csv_parquet_converter.parquet_sort_parquet(input_path_parquet=input_file, output_path_parquet=output_file, temp_directory=temp_dir, memory_limit=memory_limit)
    end=time.time()
    print(f"Execution time: {(end - start)/60:.4f} minutes")

def test_parquet_sort_csv(**kwargs): 
    small_file=True
    if small_file:
        input_file = '/users/slavska.olesia/projects/lesia/small_file/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.parquet' #2.7G
    else:
        input_file = '/users/slavska.olesia/projects/lesia/big_file/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.parquet' #7.7G
    
    if small_file:
        output_folder = '/users/slavska.olesia/projects/lesia/small_file/pq_sort_csv'
    else:
        output_folder = '/users/slavska.olesia/projects/lesia/big_file' 
    
    output_file = get_output_file_path(input_file, output_folder, new_format='csv')
    print(f"Output_file: {output_file}")

    temp_dir='/users/slavska.olesia/scratch/slavska.olesia'

    query=f"""SELECT *
            FROM read_parquet('{input_file}')
            ORDER BY chrom1, chrom2, pos1, pos2
        """

    memory_limit='200GB'
    start=time.time()
    csv_parquet_converter.parquet_applied_query_csv_gz(parquet_input_file=input_file, output_path_csv=output_file, query=query, temp_directory=temp_dir, memory_limit=memory_limit)
    end=time.time()
    print(f"Execution time: {(end - start)/60:.4f} minutes")


