"""Shared helpers for the SAS-to-Airflow ETL migration DAGs.

The five DAGs in this repo replicate data-transformation macros from the
legacy SAS macro library (``ts-sas-legacy-analytics/Macro``):

    export_csv.sas    -> export_csv_dag.py
    subset_data.sas   -> subset_data_dag.py
    transpose.sas     -> transpose_dag.py
    dedup_string.sas  -> dedup_string_dag.py
    compare.sas       -> compare_dag.py

All transformation logic lives in plain, framework-agnostic functions so it
can be unit-tested without an Airflow runtime. The DAG modules only wire these
functions into ``PythonOperator`` tasks.

Every DAG ends with a ``validate`` task built on :func:`validation_report`,
which compares input/output row counts and content checksums (the Airflow
analogue of the legacy ``%compare`` macro's PROC COMPARE step).
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Dict, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# Resolve directories relative to this file so the DAGs work both on the host
# and inside the Airflow container (where the repo's ``dags/`` folder is
# mounted at ``/opt/airflow/dags``). Override with AIRFLOW_ETL_DATA_DIR.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("AIRFLOW_ETL_DATA_DIR", os.path.join(_THIS_DIR, "data"))
INPUT_DIR = os.path.join(DATA_DIR, "input")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")


def input_path(name: str) -> str:
    return os.path.join(INPUT_DIR, name)


def output_path(name: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, name)


# ---------------------------------------------------------------------------
# Checksums / row counts
# ---------------------------------------------------------------------------
def dataframe_checksum(df: pd.DataFrame) -> str:
    """Return a deterministic content checksum for a DataFrame.

    The frame is serialized in a canonical form (columns sorted by name, rows
    sorted by their full string representation) so that the checksum is stable
    regardless of row/column ordering. This mirrors the intent of PROC COMPARE:
    two datasets holding the same values are considered equal.
    """
    if df.empty:
        canonical = "|".join(sorted(map(str, df.columns)))
        return hashlib.md5(canonical.encode("utf-8")).hexdigest()

    ordered_cols = sorted(df.columns, key=str)
    normalized = df[ordered_cols].astype(str)
    row_strings = normalized.apply(lambda r: "\x1f".join(r.values), axis=1)
    canonical = "\n".join(sorted(row_strings.tolist()))
    header = "|".join(map(str, ordered_cols))
    return hashlib.md5((header + "\n" + canonical).encode("utf-8")).hexdigest()


def file_checksum(path: str) -> str:
    """MD5 checksum of a file's raw bytes (used for flat-file outputs)."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def frame_stats(df: pd.DataFrame) -> Dict[str, object]:
    return {
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "columns": list(map(str, df.columns)),
        "checksum": dataframe_checksum(df),
    }


# ---------------------------------------------------------------------------
# Generic validation report
# ---------------------------------------------------------------------------
def validation_report(
    input_df: pd.DataFrame,
    output_df: pd.DataFrame,
    *,
    expected_row_count: Optional[int] = None,
    expect_equal_rows: bool = False,
    expect_equal_checksum: bool = False,
    extra_checks: Optional[Dict[str, bool]] = None,
) -> Dict[str, object]:
    """Compare input and output frames and raise on failed expectations.

    Always records row counts and checksums for both frames. Optional
    expectations let each DAG assert the invariant appropriate to its
    transformation (e.g. a subset must not grow, a CSV export must preserve
    every row and checksum).
    """
    report: Dict[str, object] = {
        "input": frame_stats(input_df),
        "output": frame_stats(output_df),
        "checks": {},
    }
    checks: Dict[str, bool] = {}

    if expected_row_count is not None:
        checks["output_row_count_matches_expected"] = (
            len(output_df) == expected_row_count
        )
    if expect_equal_rows:
        checks["input_output_row_count_equal"] = len(input_df) == len(output_df)
    if expect_equal_checksum:
        checks["input_output_checksum_equal"] = (
            report["input"]["checksum"] == report["output"]["checksum"]
        )
    if extra_checks:
        checks.update(extra_checks)

    report["checks"] = checks

    failed = [name for name, ok in checks.items() if not ok]
    report["passed"] = len(failed) == 0
    print(json.dumps(report, indent=2, default=str))
    if failed:
        raise ValueError(
            "Validation failed for checks: {}. Report: {}".format(
                ", ".join(failed), json.dumps(report, default=str)
            )
        )
    return report


# ---------------------------------------------------------------------------
# Default Airflow DAG args (kept here to avoid repetition across DAGs)
# ---------------------------------------------------------------------------
def default_args() -> Dict[str, object]:
    import datetime as _dt

    return {
        "owner": "data-migration",
        "depends_on_past": False,
        "start_date": _dt.datetime(2024, 1, 1),
        "retries": 0,
    }


def token_list(value: str, delimiter: str = " ") -> List[str]:
    """Split a delimited string into non-empty, stripped tokens."""
    if value is None:
        return []
    parts = str(value).split(delimiter)
    return [p.strip() for p in parts if p.strip() != ""]
