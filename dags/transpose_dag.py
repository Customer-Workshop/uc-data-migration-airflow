"""transpose_dag - Airflow port of ``Macro/transpose.sas``.

Legacy behavior
---------------
``%transpose`` wraps PROC TRANSPOSE, reshaping long data to wide:

    BY       groups that become the retained key columns (one output row each)
    VAR      the value column(s) being transposed
    ID       column whose values name the output columns
    PREFIX   optional prefix for generated column names

Port
----
This is a long->wide pivot: ``df.pivot`` on the BY key, using ID for the new
column names and VAR for the cell values. The validation task confirms the
output has one row per BY group and that no values are lost: the count of
non-null transposed cells equals the input row count, and un-pivoting the
output reproduces the input's ``(by..., id, value)`` checksum.
"""

from __future__ import annotations

from typing import List

import pandas as pd

try:
    from etl_common import (
        dataframe_checksum,
        default_args,
        input_path,
        output_path,
        validation_report,
    )
except ImportError:  # pragma: no cover
    from dags.etl_common import (
        dataframe_checksum,
        default_args,
        input_path,
        output_path,
        validation_report,
    )

SOURCE = "vitals_long.csv"
TARGET = "vitals_wide.csv"
BY: List[str] = ["studyid", "usubjid", "visit"]
ID = "measure"
VAR = "value"


def transpose(
    df: pd.DataFrame, *, by: List[str], id_col: str, var: str, prefix: str = ""
) -> pd.DataFrame:
    """Replicate PROC TRANSPOSE (long -> wide) for a single VAR column."""
    wide = (
        df.pivot_table(index=by, columns=id_col, values=var, aggfunc="first")
        .reset_index()
    )
    wide.columns.name = None
    if prefix:
        wide = wide.rename(
            columns={c: "{}{}".format(prefix, c) for c in wide.columns if c not in by}
        )
    return wide


def untranspose(wide: pd.DataFrame, *, by: List[str], id_col: str, var: str) -> pd.DataFrame:
    """Inverse pivot used only for validation (wide -> long)."""
    long = wide.melt(id_vars=by, var_name=id_col, value_name=var).dropna(subset=[var])
    return long.reset_index(drop=True)


# --- Airflow task callables --------------------------------------------------
def _transform(**_):
    df = pd.read_csv(input_path(SOURCE))
    wide = transpose(df, by=BY, id_col=ID, var=VAR)
    wide.to_csv(output_path(TARGET), index=False)


def _validate(**_):
    src = pd.read_csv(input_path(SOURCE))
    out = pd.read_csv(output_path(TARGET))

    expected_rows = src[BY].drop_duplicates().shape[0]
    value_cells = out.drop(columns=BY).notna().to_numpy().sum()

    # Reconstruct the long form and compare against the input on (by, id, value).
    reconstructed = untranspose(out, by=BY, id_col=ID, var=VAR)
    src_key = src[BY + [ID, VAR]].copy()
    src_key[VAR] = src_key[VAR].astype(str)
    reconstructed[VAR] = reconstructed[VAR].astype(str)

    validation_report(
        src,
        out,
        expected_row_count=expected_rows,
        extra_checks={
            "no_values_lost": int(value_cells) == len(src),
            "roundtrip_checksum_matches": dataframe_checksum(reconstructed)
            == dataframe_checksum(src_key),
        },
    )


# --- DAG ---------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="transpose",
        description="SAS transpose.sas -> long-to-wide pivot (PROC TRANSPOSE)",
        default_args=default_args(),
        schedule_interval=None,
        catchup=False,
        tags=["sas-migration", "transpose"],
    ) as dag:
        transform = PythonOperator(task_id="transpose", python_callable=_transform)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        transform >> validate
except ImportError:  # pragma: no cover
    dag = None
