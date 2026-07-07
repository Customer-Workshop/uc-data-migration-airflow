"""export_csv_dag - Airflow port of ``Macro/export_csv.sas``.

Legacy behavior
---------------
``%export_csv`` is a thin wrapper over ``%export_dlm(dbms=csv)`` that writes a
SAS dataset to a delimited (comma) flat file. Relevant options:

    DATA     input dataset (a WHERE clause may be embedded)
    PATH     output file path
    REPLACE  overwrite an existing file? (default N)
    LABEL    use column labels instead of names for the header
    HEADER   emit a header row? (default Y)
    LRECL    logical record length (not meaningful for pandas; ignored)

Port
----
Read the input dataset, optionally apply a WHERE-style filter, and write it out
as CSV honoring the ``replace``, ``label`` and ``header`` options.
The validation task confirms every input row survives to the output file and
that the round-tripped content checksum is unchanged.
"""

from __future__ import annotations

import os

import pandas as pd

try:
    from etl_common import (
        default_args,
        input_path,
        output_path,
        validation_report,
    )
except ImportError:  # pragma: no cover - allow ``dags.`` package import
    from dags.etl_common import (
        default_args,
        input_path,
        output_path,
        validation_report,
    )

SOURCE = "accounts.csv"
TARGET = "accounts_export.csv"
LABELS = {
    "account_id": "Account ID",
    "customer_name": "Customer Name",
    "product": "Product Type",
    "balance": "Current Balance",
    "status": "Account Status",
    "open_date": "Open Date",
}


def export_csv(
    df: pd.DataFrame,
    dest: str,
    *,
    where: str = None,
    replace: bool = False,
    header: bool = True,
    label: bool = False,
) -> pd.DataFrame:
    """Replicate ``%export_csv`` / ``%export_dlm(dbms=csv)``."""
    out = df.copy()
    if where:
        out = out.query(where).reset_index(drop=True)

    if os.path.exists(dest) and not replace:
        raise FileExistsError(
            "{} already exists and replace=N (matches SAS REPLACE=N)".format(dest)
        )

    to_write = out.copy()
    if label:
        to_write = to_write.rename(columns={c: LABELS.get(c, c) for c in to_write.columns})

    to_write.to_csv(dest, index=False, header=header)
    return out


# --- Airflow task callables --------------------------------------------------
def _transform(**_):
    df = pd.read_csv(input_path(SOURCE))
    export_csv(df, output_path(TARGET), replace=True, header=True, label=False)


def _validate(**_):
    src = pd.read_csv(input_path(SOURCE))
    out = pd.read_csv(output_path(TARGET))
    validation_report(
        src,
        out,
        expect_equal_rows=True,
        expect_equal_checksum=True,
    )


# --- DAG ---------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="export_csv",
        description="SAS export_csv.sas -> write dataset to CSV flat file",
        default_args=default_args(),
        schedule_interval=None,
        catchup=False,
        tags=["sas-migration", "export"],
    ) as dag:
        transform = PythonOperator(task_id="export_to_csv", python_callable=_transform)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        transform >> validate
except ImportError:  # pragma: no cover - Airflow not installed (unit tests)
    dag = None
