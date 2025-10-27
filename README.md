# pairs_to_parquet
[![Documentation Status](https://pairs-to-parquet.readthedocs.io/en/latest/)](https://pairs-to-parquet.readthedocs.io/en/latest/)
[![Join the chat on Slack](https://img.shields.io/badge/chat-slack-%233F0F3F?logo=slack)](https://bit.ly/2UaOpAe)


## Transform 3D contacts (.pairs) to parquet & process them

`pairs_to_parquet` is a .parquet extention of .pairs file format.

The main purpose of this extension is to leverage the row groups and metadata features of the Parquet format in order to:

- speed up data selection, filtering and sorting
- address a minor limitation of the .pairs format, where metadata cannot be easily parsed by generic CSV readers
- reduce storage space required for the data 
- improve I/O performance



## Data formats

There are 2 main file formats, which are used by our converter & processor: 

1. `.pairs`: 
`pairtools` produce and operate on tab-separated files compliant with the [.pairs](https://github.com/4dn-dcic/pairix/blob/master/pairs_format_specification.md) format defined by the [4D Nucleome Consortium](https://www.4dnucleome.org/). All pairtools properly manage file headers and keep track of the data processing history.

2. `.parquet`: 
a columnar or hybrid file format, which is highly optimized for big data processing. It supports features like predicate pushdown and column projection -> better query performance by minimizing data read from disk.
In our workflow, .parquet files are processed using [duckdb](https://duckdb.org/docs/stable/data/parquet/overview) - an open-source column-oriented Relational Database Management System. 
For more information, see [parquet metadata](https://duckdb.org/docs/stable/data/parquet/metadata) by duckdb

## Operations: 
- convert .pairs -> .parquet
- convert .parquet -> .pairs
- sort pairs in lexycographic order

## Installation

Requirements:
- Python 3.x

Currently there is only 1 option for installing `pairs_to_parquet`:

And it is the same, when you want to modify `pairs_to_parquet`: build `pairs_to_parquet` from source via pip's "editable" mode:

```sh
$ git clone https://github.com/ayaksvals/pairs_to_parquet
$ cd pairs_to_parquet
$ pip install -e .
```

## Tools

- `csv_to_parquet`: transform standard .pairs files into the optimized .parquet format for faster querying and reduced storage. Header of .pairs becomes key-value metadata in a new parquet file

- `parquet_to_csv`: export Parquet data back into .pairs format for compatibility with existing pairtools pipelines.

- `sort`: sort .pairs or .parquet files(the lexicographic order for chromosomes, the numeric order for the positions, the lexicographic order for pair types)


## Why to use `.parquet` extention for sorting (and many more future processing tools)?
If we use the same 2.4 GB file, 35 GB of memory, 4 threads:



| Tool, input & output formats          | Memory (2.4GB)| real time | user time | sys time  |                              Comments                                 |
|---------------------------------------|---------------|-----------|-----------|-----------|-----------------------------------------------------------------------|
| Pairtools sort                        |     2.3GB     | 10min 10s | 20min 23s | 3min 14s  |                                                                       |
| pairs_to_parquet csv-parquet sort     |     2.5GB     | 2min  24s | 6min  56s | 0min 47s  | 5x times faster in real time, 3x times faster in user time            |
| pairs_to_parquet parquet-parquet sort |     2.6GB     | 2min  33s | 6min  18s | 2m   6s   | also a major speed up                                                 |
| pairs_to_parquet csv-csv sort         |     2.2GB     | 4mim  39s | 15min 55s | 0min 53s  | worse, that first 2; better, than pairtools sort; better compression  |
| pairs_to_parquet parquet-csv sort     |     2.6GB     | 5min  15s | 14min 10s | 1m   56s  | worse, that first 2, but still better, than pairtools sort            |

So pairs_to_parquet with any input and output format will outperform pairtools sort on csv. Here csv-parquet and parquet-parquet show the best results. 
Spoiler alert: on bigger files, like 10GB compressed, the difference feels even more dramatic. pairtools sort ~25 min, pairs_to_parquet sort csv-parquet ~12 minutes. 

Working directly with Parquet files (parquet → parquet sort) delivers performance close to the best case, confirming that the Parquet format maintains efficiency across repeated operations.

As a result, switching from .pairs (CSV) to .parquet for sorting (and we will show in the future other data processing) yields 3–4× faster runtimes, better I/O performance, and improved scalability for large datasets.

So welcome to the world of Parquet, it is been waiting for you! 