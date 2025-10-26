import duckdb
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from itertools import product

from pairtools.lib import pairsam_format
from . import json_transform, header_metadata

# MAYBE TO RENAME TO PARQUET UTILS WILL BE MORE STRAIGHTFORWARD

# duckdb
def setup_duckdb_connection(temp_directory=None, memory_limit=None, enable_progress_bar=True, enable_profiling='json', numb_threads=4):
    """
    Sets up a DuckDB connection with specified parameters.

    Parameters
    ----------
    temp_directory (str): Path to the temporary directory for DuckDB.
    memory_limit (str): Memory limit for DuckDB operations (e.g., '55GB').
    enable_progress_bar (bool): Whether to enable the progress bar. Defaults to True.
    enable_profiling (str): Controlling the level of detail in the profiling output. Choose from: query_tree, json, query_tree_optimizer, no_output
    numb_threads (int): The amount of threads to paralilize the work

    Returns
    ----------
    duckdb.DuckDBPyConnection: Configured DuckDB connection.
    """
    con = duckdb.connect(":memory:")
    
    if temp_directory is not None:
        con.execute(f"PRAGMA temp_directory='{temp_directory}';")

    if memory_limit is not None: 
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")

    con.execute(f"SET enable_progress_bar = {'true' if enable_progress_bar else 'false'};")
    con.execute(f"PRAGMA enable_profiling = {enable_profiling};")

    con.execute(f"SET threads = {numb_threads};")
    return con

# duckdb
def setup_duckdb_types(con, chromosom_field, reads_type_enum=False):
    """
    Sets up ENUM types in a DuckDB connection.

    Parameters
    ----------
    con (duckdb.DuckDBPyConnection): The DuckDB connection.
    chromosom_field (tuple): The ENUM values for CHROM_TYPE.
    reads_type_enum (bool): if reads_type is always '.', then it is possible to use it and speed up operations

    Returns:
    ----------
    con (duckdb.DuckDBPyConnection): Configured DuckDB connection.

    """
    # Drop existing types if they exist
    con.execute("DROP TYPE IF EXISTS CHROM_TYPE")
    con.execute("DROP TYPE IF EXISTS READS_TYPE")
    con.execute("DROP TYPE IF EXISTS STRAND_TYPE")
    con.execute("DROP TYPE IF EXISTS ALIGNMENT_TYPE")

    # Create new ENUM types
    con.execute(f"CREATE TYPE CHROM_TYPE AS ENUM {chromosom_field};")
    con.execute("CREATE TYPE STRAND_TYPE AS ENUM ('+', '-');")

    # all the possible alignments we can get 
    ALIGNMENT_TYPE = tuple(a + b for a, b in product(["U", "u", "m", "M", "R", "N"], repeat=2)) + ("DD", "WW", "XX", "!")
    con.execute(f"CREATE TYPE ALIGNMENT_TYPE AS ENUM {ALIGNMENT_TYPE};")

    if reads_type_enum:
        con.execute("CREATE TYPE READS_TYPE AS ENUM ('.');") 

    return con

# duckdb
def classify_column_types_by_name(column_names):
    """
    Classify columns based on predefined rules and types.

    Parameters
    ----------
    column_names (list): A list of column names to classify.
    
    Returns
    ----------
    dict: The updated column_types dictionary with column name as a key and Enum or other types as values.
    """

    column_types = {}
    for col in column_names:
        if col in ["chrom1", "chrom2"]:
            column_types[col] = "CHROM_TYPE"
        elif col in ["strand1", "strand2"]:
            column_types[col] = "STRAND_TYPE"
        elif col == "pair_type":
            column_types[col] = "ALIGNMENT_TYPE"
        elif col in pairsam_format.DTYPES_PAIRSAM:
            column_types[col] = "INTEGER" if pairsam_format.DTYPES_PAIRSAM[col] == int else "STRING"
        elif col in pairsam_format.DTYPES_EXTRA_COLUMNS:
            column_types[col] = "INTEGER" if pairsam_format.DTYPES_EXTRA_COLUMNS[col] == int else "STRING"
        else:
            column_types[col] = "STRING"

    return column_types


# duckdb
def extract_duckdb_metadata(parquet_file, con=None):
    """
    Extracts key-value metadata embedded in a Parquet file using DuckDB.

    Parameters
    ----------
    parquet_file (str): The path to the Parquet file to be read.
    con (duckdb.DuckDBPyConnection): Configured DuckDB connection. If no DuckDB connection is provided, it defaults to using the duckdb module directly.

    Returns
    ----------
    pandas.DataFrame with 2 columns: 
        key (str): The metadata key, 
        value (str): The corresponding metadata value.
    """
    if con==None:
        con=duckdb

    # Query to extract key-value metadata
    query = f"""
        SELECT key::TEXT AS key, value::TEXT AS value
        FROM parquet_kv_metadata('{parquet_file}');
    """

    metadata = con.query(query).fetchdf()
    return metadata

#duckdb
def decode_parquet_metadata_duckdb_as_dict(metadata):
    """ 
    Decodes and parses the JSON-formatted metadata values from a DuckDB-extracted Parquet metadata DataFrame

    Parameters
    ----------
    metadata (pandas.DataFrame): a df containing at least two columns:
        key: Metadata key names.
        value: JSON-encoded metadata values (strings).
    
    Returns
    ----------
    (dict): keys are same as in metadata, and each value is the decoded Python object from the JSON-formatted metadata value.
    """
    # Apply the decoding function to the 'value' column
    metadata['parsed_value'] = metadata['value'].apply(json_transform.decode_and_parse_json)
    
    metadata_dict = dict(zip(metadata['key'], metadata['parsed_value']))
    return metadata_dict



def header_to_kv_metadata(header):
    field_names = header_metadata.extract_field_names(header)
    header_json_dict = json_transform.header_to_json_dict(header, field_names)
    kv_metadata = json_transform.json_dict_to_json_str(header_json_dict)
    return kv_metadata


def duckdb_kv_metadata_to_header(parquet_input_path, con=None):
    metadata = extract_duckdb_metadata(parquet_input_path, con)
    metadata_dict = decode_parquet_metadata_duckdb_as_dict(metadata)
    header = header_metadata.metadata_dict_to_header_list(metadata_dict) 
    return header


def parquet_file_iterator(parquet_file_path):
    """
    Yields pyarrow.Tables from a parquet file row group by row group

    Parameters:
    ----------
    parquet_file_path (str): The path to the Parquet file to be read.
    
    Returns
    ----------
    Iterator[pyarrow.Table]: An iterator that yields one pyarrow.Table per row group from the Parquet file.
    """
    parquet_file = pq.ParquetFile(parquet_file_path)
    for i in range(parquet_file.num_row_groups):
        yield parquet_file.read_row_group(i)

def duckdb_query_iterator(con, query):
    """
    Yields pyarrow.Tables from a duckdb query in batches

    Parameters:
    ----------
    con (duckdb.DuckDBPyConnection): An active DuckDB connection object used to execute the query.
    query (str): The SQL query string to execute.

    Returns
    ----------
    Iterator[pyarrow.Table]: An iterator that yields query results as pyarrow.Table objects, one per batch.
    """
    for batch in con.execute(query).fetch_record_batch():
        yield pa.Table.from_batches([batch])


def write_parquet_to_csv(parquet_iterator, sink):
    """
    Writes data from a Parquet iterator to a CSV file (or other writable sink) in batches.

    Parameters:
    ----------
    parquet_iterator (Iterator[pyarrow.Table]): iterator that yields pyarrow.Table objects, (from a Parquet file or query result)
    sink (str or pyarrow.NativeFile): destination to write the CSV data to

    Returns
    ----------
    
    """
    # hard-coded tab separator to follow the DCIC pairs standard
    write_options = csv.WriteOptions(
    include_header=False,
    delimiter="\t",     
    quoting_style="none" 
)
    for batch in parquet_iterator:
        csv.write_csv(batch, sink, write_options=write_options)

def sort_query(columns_to_sort):
    query=f""" ORDER BY """ +", ".join(columns_to_sort)
    return query


