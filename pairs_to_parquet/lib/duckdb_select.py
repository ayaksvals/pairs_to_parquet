
import duckdb
import re
import warnings
from pairtools.lib import fileio, headerops, pairsam_format

from . import duckdb_utils, json_transform, header_metadata

def translate_condition(cond: str) -> str:
    """Translate Pairtools/Python-like expressions into DuckDB SQL."""

    # Basic operators
    cond = cond.replace(" and ", " AND ")
    cond = cond.replace(" or ", " OR ")
    cond = cond.replace("==", "=")
    cond = cond.replace("!=", "<>")
    cond = cond.replace(" in ", " IN ")

    # anything inside parentheses (...) becomes a group -> \1, \2..
    # Python abs() → DuckDB abs() -> USELESS??
    cond = re.sub(r"abs\(([^)]+)\)", r"abs(\1)", cond)

    # String quotes -> SQL single quotes
    cond = re.sub(r'"([^"]*)"', r"'\1'", cond)

    # regex_match(col, 'pattern')
    cond = re.sub(
        r"regex_match\(([^,]+),\s*'([^']+)'\)",
        r"\1 ~ '\2'",
        cond,
        flags=re.IGNORECASE,
    )

    # wildcard_match(col, 'C*') → col LIKE 'C%'
    cond = re.sub(
        r"wildcard_match\(([^,]+),\s*'([^']+)'\)",
        lambda m: f"{m.group(1)} LIKE '{m.group(2).replace('*','%')}'",
        cond,
        flags=re.IGNORECASE,
    )

    # csv_match(col, 'a,b,c')
    def csv_match_repl(m):
        col = m.group(1)
        values = m.group(2).split(",")
        quoted = ",".join(f"'{v.strip()}'" for v in values)
        return f"{col} IN ({quoted})"

    cond = re.sub(
        r"csv_match\(([^,]+),\s*'([^']+)'\)",
        csv_match_repl,
        cond,
        flags=re.IGNORECASE,
    )

    # region_match(col, pos, 'chr', start, end)
    cond = re.sub(
        r"region_match\(([^,]+),([^,]+),\s*'([^']+)',\s*(\d+),\s*(\d+)\)",
        r"(\1 = '\3' AND \2 BETWEEN \4 AND \5)",
        cond,
        flags=re.IGNORECASE,
    )

    # region_match(col, pos, 'chr', start)
    # → open-ended range
    cond = re.sub(
        r"region_match\(([^,]+),([^,]+),\s*'([^']+)',\s*(\d+)\)",
        r"(\1 = '\3' AND \2 >= \4)",
        cond,
        flags=re.IGNORECASE,
    )
    return cond

def header_update(header:list[str],
    UTIL_NAME: str, 
    remove_columns: str = "",
    chrom_subset: str = None,
    ):
    new_header = headerops.append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)

    if remove_columns:
        input_columns = headerops.extract_column_names(header)
        remove_columns = remove_columns.split(",")
        for col in remove_columns:
            if col in pairsam_format.COLUMNS_PAIRS:
                warnings.warn(
                    f"Removing required {col} column for .pairs format. Output is not .pairs anymore"
                )
            elif col in pairsam_format.COLUMNS_PAIRSAM:
                warnings.warn(
                    f"Removing required {col} column for .pairsam format. Output is not .pairsam anymore"
                )
        updated_columns = [x for x in input_columns if x not in remove_columns]

        if len(updated_columns) == len(input_columns):
            warnings.warn(
                f"Some column(s) {','.join(remove_columns)} not in the file, the operation has no effect"
            )
        else:
            new_header = headerops.set_columns(new_header, updated_columns)
    
    new_chroms = None
    if chrom_subset is not None:
        new_chroms = [l.strip().split("\t")[0] for l in open(chrom_subset, "r")]

    if new_chroms is not None:
        new_header = headerops.subset_chroms_in_pairsheader(new_header, new_chroms)
    
    return new_header

def run_select_parquet(
    input_path: str,
    output: str,
    output_rest: str,
    condition: str,
    remove_columns: str = "",
    chrom_subset: str = None,
    type_cast=(),
):
    """Execute the SELECT operation using DuckDB SQL."""

    UTIL_NAME="pairs_to_parquet_select"

    con = duckdb.connect()
    old_header=duckdb_utils.duckdb_kv_metadata_to_header(input_path, con)
    new_header=header_update(old_header, UTIL_NAME, remove_columns, chrom_subset)

    sql_condition = translate_condition(condition.strip())

    if chrom_subset:
        with open(chrom_subset) as f:
            chroms = [l.split()[0] for l in f]
        sql_condition = (
            f"({sql_condition}) AND chrom1 IN {tuple(chroms)} "
            f"AND chrom2 IN {tuple(chroms)}"
        )

    # initial query
    query = f"SELECT * FROM parquet_scan('{input_path}') WHERE {sql_condition}"

    if remove_columns:
        # because they were already updated in header update
        keep = headerops.extract_column_names(new_header)
        if not keep:
            raise ValueError("remove-columns removed all columns.")

        query = f"SELECT {', '.join(keep)} FROM parquet_scan('{input_path}') WHERE {sql_condition}"

    if type_cast:
        cast_exprs = []
        for col, typ in type_cast:
            cast_exprs.append(f"CAST({col} AS {typ}) AS {col}")

        cast_select = ", ".join(cast_exprs)
        query = query.replace("SELECT", f"SELECT {cast_select},", 1)

    # write to output
    kv_metadata = duckdb_utils.header_to_kv_metadata(new_header)
    con.execute(f"COPY ({query}) TO '{output}' (FORMAT PARQUET, KV_METADATA {kv_metadata})")

    # write rest
    if output_rest:
        rest_query = f"""
        SELECT * FROM parquet_scan('{input_path}')
        EXCEPT ALL
        ({query})
        """
        con.execute(f"COPY ({rest_query}) TO '{output_rest}' (FORMAT PARQUET, KV_METADATA {kv_metadata})")

