import io
import sys
import click

from ..lib import duckdb_select
from . import cli, common_io_options



@cli.command()
@click.argument("condition", type=str)
@click.argument("parquet_path", type=str, required=True)
@click.option(
    "-o",
    "--output",
    type=str,
    required=True,
    help="Output Parquet file for selected pairs.",
)
@click.option(
    "--output-rest",
    type=str,
    default=None,
    help="Optional output Parquet file for non-selected pairs.",
)
@click.option(
    "--chrom-subset",
    type=str,
    default=None,
    help="Path to chromnames file (1st column = chromosome).",
)
@click.option(
    "-t",
    "--type-cast",
    type=(str, str),
    default=(),
    multiple=True,
    help="Column type casts, e.g. -t pos1 INT",
)
@click.option(
    "--remove-columns",
    "-r",
    type=str,
    default="",
    help="Comma-separated list of columns to drop.",
)
@common_io_options
def select(
    condition,
    parquet_path,
    output,
    output_rest,
    chrom_subset,
    type_cast,
    remove_columns,
    **kwargs,
):
    """Select pairs from a Parquet file according to CONDITION.

    CONDITION is a Python-like boolean expression, e.g.:

        'pair_type == \"UU\"'
        'chrom1 == chrom2 and abs(pos1 - pos2) < 1e6'
        'regex_match(chrom1, \"chr[0-9]+\")'
        'region_match(chrom1, pos1, \"chr1\", 1000, 5000)'

    This tool reproduces `pairtools select`, but works on Parquet using DuckDB.
    """
    duckdb_select.run_select_parquet(
        input_path=parquet_path,
        output=output,
        output_rest=output_rest,
        condition=condition,
        remove_columns=remove_columns,
        chrom_subset=chrom_subset,
        type_cast=type_cast,
    )


if __name__ == "__main__":
    select()
