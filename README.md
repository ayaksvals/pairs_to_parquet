# pairs_to_parquet
[![Join the chat on Slack](https://img.shields.io/badge/chat-slack-%233F0F3F?logo=slack)](https://bit.ly/2UaOpAe)


## Transform 3D contacts (.pairs) to parquet & process them

`pairs_to_parquet` is a .parquet extention of .pairs file format.

The main purspose of this extention is to use the row groups of .parquet file format and their metadata, in order to 

- speed up selection, filtering and sorting
- solve the minor problem of .pairs: metadata not easily parsed by generic CSV readers
- reduce the storage space required for the data 
- improve the I/O performance



## Data formats

There are 2 main file formats, which are used by our converter & processor: 

1. `.pairs`: `pairtools` produce and operate on tab-separated files compliant with the [.pairs](https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md) format defined by the [4D Nucleome Consortium](https://www.4dnucleome.org/). All pairtools properly manage file headers and keep track of the data processing history.

2. `.parquet`: 
a columnar or hybrid file format, which is highly optimized for big data processing. Has features like predicate pushdown and column projection -> better query performance by min data read from disk.
Here we process .parquet file format with [duckdb](https://duckdb.org/docs/stable/data/parquet/overview) - an open-source column-oriented Relational Database Management System. 
More information about processing [parquet metadata](https://duckdb.org/docs/stable/data/parquet/metadata) by duckdb

## Operations: 
- convert .pairs to .parquet
- convert .parquet to pairs
- sort pairs in lexycographic order

## Installation

Requirements:
- Python 3.x

Currently there is only 1 option for installing `pairs_to_parquet`:

When you want to modify `pairs_to_parquet`, build `pairs_to_parquet` from source via pip's "editable" mode:

```sh
$ git clone https://github.com/ayaksvals/pairs_to_parquet
$ cd pairs_to_parquet
$ pip install -e .
```

## Tools

- `csv_to_parquet`: converting the .pairs file format to a .parquet file format. Header of .pairs becomes key-value metadata in a new parquet file

- `parquet_to_csv`: converting back .parquet file format back to the .pairs file format

- `sort`: sort .pairs or .parquet files(the lexicographic order for chromosomes, the numeric order for the positions, the lexicographic order for pair types)


# Why to use `pairs_to_parquet` for sorting (and many more future processing tools)?

Same 2.4 GB file, 35 GB of memory, 4 threads:
pairtools sort: 
real: 10 min
user: 20 min 
sys: 3 min



