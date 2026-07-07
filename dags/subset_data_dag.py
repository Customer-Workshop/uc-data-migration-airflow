"""subset_data_dag - Airflow port of ``Macro/subset_data.sas``.

Legacy behavior
---------------
``%subset_data`` subsets a dataset by rows and columns and can rename columns:

    WHERE     row filter (SAS WHERE expression)
    IF        subsetting IF (semantically identical here to WHERE)
    FIRSTOBS  first observation to keep (1-based)
    LASTOBS   last observation to keep (1-based, inclusive)
    OBS       non-contiguous 1-based ranges, e.g. "1-5 or 11-15"
    KEEP      columns to keep
    DROP      columns to drop
    RENAME    "old=new old2=new2" (applied first; later options use new names)

Port
----
Rows are filtered by OBS ranges / FIRSTOBS-LASTOBS / WHERE, then columns are
renamed and projected with KEEP/DROP. The validation task confirms the output
never grows beyond the input, that every output row is a genuine input row
(subset invariant), and that the kept columns' checksum matches the input
restricted to those same rows/columns.
"""

from __future__ import annotations

import re
from typing import List, Tuple

import pandas as pd

try:
    from etl_common import default_args, input_path, output_path, validation_report
except ImportError:  # pragma: no cover
    from dags.etl_common import default_args, input_path, output_path, validation_report

SOURCE = "accounts.csv"
TARGET = "accounts_subset.csv"

# Demo configuration mirroring a typical %subset_data call:
#   keep only ACTIVE accounts, rename balance->current_balance,
#   keep a projection of columns.
WHERE = 'status == "ACTIVE"'
RENAME = {"balance": "current_balance"}
KEEP = ["account_id", "customer_name", "product", "current_balance"]


def _parse_obs(obs: str, n_rows: int) -> List[int]:
    """Parse a SAS OBS spec like ``1-5 or 11-15`` into 0-based row indices."""
    keep: List[int] = []
    for chunk in re.split(r"\s+or\s+", obs.strip(), flags=re.IGNORECASE):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            lo, hi = (int(x) for x in chunk.split("-", 1))
        else:
            lo = hi = int(chunk)
        for one_based in range(lo, hi + 1):
            if 1 <= one_based <= n_rows:
                keep.append(one_based - 1)
    return keep


def subset_data(
    df: pd.DataFrame,
    *,
    rename: dict = None,
    where: str = None,
    obs: str = None,
    firstobs: int = None,
    lastobs: int = None,
    keep: List[str] = None,
    drop: List[str] = None,
) -> pd.DataFrame:
    """Replicate ``%subset_data``. RENAME is applied first (as in the macro)."""
    out = df.copy()
    if rename:
        out = out.rename(columns=rename)

    # Row selection (dataset options FIRSTOBS/OBS applied on the SET statement).
    if obs:
        out = out.iloc[_parse_obs(obs, len(out))]
    elif firstobs is not None or lastobs is not None:
        start = (firstobs - 1) if firstobs else 0
        end = lastobs if lastobs else len(out)
        out = out.iloc[start:end]

    if where:
        out = out.query(where)

    out = out.reset_index(drop=True)

    # Column selection.
    if keep:
        out = out[[c for c in keep if c in out.columns]]
    if drop:
        out = out.drop(columns=[c for c in drop if c in out.columns])
    return out


def _renamed_input() -> Tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_csv(input_path(SOURCE))
    renamed = src.rename(columns=RENAME) if RENAME else src.copy()
    return src, renamed


# --- Airflow task callables --------------------------------------------------
def _transform(**_):
    src = pd.read_csv(input_path(SOURCE))
    out = subset_data(src, rename=RENAME, where=WHERE, keep=KEEP)
    out.to_csv(output_path(TARGET), index=False)


def _validate(**_):
    _, renamed = _renamed_input()
    out = pd.read_csv(output_path(TARGET))

    # Expected: input restricted to the WHERE rows and KEEP columns.
    expected = renamed.query(WHERE).reset_index(drop=True)[KEEP]

    from etl_common import dataframe_checksum  # local import to avoid cycle at import time

    validation_report(
        renamed,
        out,
        extra_checks={
            "output_not_larger_than_input": len(out) <= len(renamed),
            "row_count_matches_where_filter": len(out) == len(expected),
            "kept_columns_checksum_matches": dataframe_checksum(out)
            == dataframe_checksum(expected),
        },
    )


# --- DAG ---------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="subset_data",
        description="SAS subset_data.sas -> filter rows/columns + rename",
        default_args=default_args(),
        schedule_interval=None,
        catchup=False,
        tags=["sas-migration", "subset"],
    ) as dag:
        transform = PythonOperator(task_id="subset", python_callable=_transform)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        transform >> validate
except ImportError:  # pragma: no cover
    dag = None
