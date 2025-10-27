import io
import json
import pytest
import duckdb
import pyarrow as pa
from unittest.mock import MagicMock, patch

from pairs_to_parquet.lib.duckdb_utils import (
    setup_duckdb_connection,
    setup_duckdb_types,
    classify_column_types_by_name,
    decode_parquet_metadata_duckdb_as_dict,
    header_to_kv_metadata,
    duckdb_kv_metadata_to_header,
    write_parquet_to_csv,
    sort_query,
)


# --------------------------------------------------------------------
# TEST setup_duckdb_connection
# --------------------------------------------------------------------
def test_setup_duckdb_connection_basic(tmp_path):
    con = setup_duckdb_connection(
        temp_directory=str(tmp_path),
        memory_limit="256MiB",
        enable_progress_bar=False,
        enable_profiling="query_tree",
        numb_threads=2,
    )
    assert isinstance(con, duckdb.DuckDBPyConnection)
    # Check PRAGMAs actually took effect
    assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "256.0 MiB"
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    assert int(threads) == 2


# --------------------------------------------------------------------
# TEST setup_duckdb_types
# --------------------------------------------------------------------
def test_setup_duckdb_types_enum_creation():
    con = duckdb.connect(":memory:")
    chroms = ("chr1", "chr2", "chrX")
    con = setup_duckdb_types(con, chroms, reads_type_enum=True)
    # Verify created types exist
    res = con.execute("SELECT unnest(enum_range(NULL::CHROM_TYPE))").fetchall()
    assert ("chr1",) in res
    assert ("chrX",) in res
    # Strand type check
    assert ("+",) in con.execute("SELECT unnest(enum_range(NULL::STRAND_TYPE))").fetchall()
    # Reads type should exist when flag True
    assert ("." ,) in con.execute("SELECT unnest(enum_range(NULL::READS_TYPE))").fetchall()


# --------------------------------------------------------------------
# TEST classify_column_types_by_name
# --------------------------------------------------------------------
def test_classify_column_types_by_name_with_known_and_unknown(monkeypatch):
    mock_pairsam = {"pos1": int, "name1": str}
    mock_extra = {"custom": int}
    monkeypatch.setattr("pairtools.lib.pairsam_format.DTYPES_PAIRSAM", mock_pairsam)
    monkeypatch.setattr("pairtools.lib.pairsam_format.DTYPES_EXTRA_COLUMNS", mock_extra)

    cols = ["chrom1", "strand2", "pair_type", "pos1", "custom", "unknown"]
    result = classify_column_types_by_name(cols)

    assert result["chrom1"] == "CHROM_TYPE"
    assert result["strand2"] == "STRAND_TYPE"
    assert result["pair_type"] == "ALIGNMENT_TYPE"
    assert result["pos1"] == "INTEGER"
    assert result["custom"] == "INTEGER"
    assert result["unknown"] == "STRING"


# --------------------------------------------------------------------
# TEST decode_parquet_metadata_duckdb_as_dict
# --------------------------------------------------------------------
def test_decode_parquet_metadata_duckdb_as_dict(monkeypatch):
    import pandas as pd
    df = pd.DataFrame({
        "key": ["columns", "shape"],
        "value": ['["a", "b"]', '"triangular"']
    })
    # Mock json_transform.decode_and_parse_json to just json.loads
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.json_transform.decode_and_parse_json", json.loads)
    result = decode_parquet_metadata_duckdb_as_dict(df)
    assert result["columns"] == ["a", "b"]
    assert result["shape"] == "triangular"


# --------------------------------------------------------------------
# TEST header_to_kv_metadata
# --------------------------------------------------------------------
def test_header_to_kv_metadata(monkeypatch):
    mock_header = ["## columns: a b c"]
    # Mock both dependencies
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.header_metadata.extract_field_names", lambda h: ["columns"])
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.json_transform.header_to_json_dict", lambda h, f: {"columns": '["a","b","c"]'})
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.json_transform.json_dict_to_json_str", lambda d: {"columns": '["a","b","c"]'})

    result = header_to_kv_metadata(mock_header)
    assert result == {"columns": '["a","b","c"]'}


# --------------------------------------------------------------------
# TEST duckdb_kv_metadata_to_header
# --------------------------------------------------------------------
def test_duckdb_kv_metadata_to_header(monkeypatch):
    fake_metadata_df = MagicMock(name="df")
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.extract_duckdb_metadata", lambda path, con: fake_metadata_df)
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.decode_parquet_metadata_duckdb_as_dict", lambda df: {"columns": ["a", "b"]})
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.header_metadata.metadata_dict_to_header_list", lambda d: ["#columns: a b"])

    result = duckdb_kv_metadata_to_header("fake.parquet")
    assert result == ["#columns: a b"]


# --------------------------------------------------------------------
# TEST write_parquet_to_csv
# --------------------------------------------------------------------
def test_write_parquet_to_csv(monkeypatch):
    # Create fake iterator of pyarrow.Tables
    table = pa.table({"x": [1, 2], "y": [3, 4]})
    iterator = [table]
    fake_write = MagicMock()
    monkeypatch.setattr("pairs_to_parquet.lib.duckdb_utils.csv.write_csv", fake_write)

    sink = io.StringIO()
    from pairs_to_parquet.lib.duckdb_utils import write_parquet_to_csv
    write_parquet_to_csv(iterator, sink)

    fake_write.assert_called_once()


# --------------------------------------------------------------------
# TEST sort_query
# --------------------------------------------------------------------
def test_sort_query():
    result = sort_query(["chrom1", "pos1"])
    assert result.strip().startswith("ORDER BY")
    assert "chrom1" in result and "pos1" in result
