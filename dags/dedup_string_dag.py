"""dedup_string_dag - Airflow port of ``Macro/dedup_string.sas``.

Legacy behavior
---------------
``%dedup_string`` runs inside a DATA step and removes duplicate tokens from a
delimited string, preserving first-occurrence order. Comparison is
case-insensitive (``indexw(upcase(...))``) but the original token text is kept.

    INVAR    input variable holding the string
    OUTVAR   output variable (defaults to INVAR, i.e. in place)
    DLM      token delimiter (defaults to a space)

Port
----
Apply an order-preserving, case-insensitive dedup to a string column. Row count
is unchanged (the macro is per-observation). The validation task confirms row
counts match, that no output string contains a duplicate token, and that every
output token was present in the corresponding input string (subset invariant).
"""

from __future__ import annotations

import pandas as pd

try:
    from etl_common import default_args, input_path, output_path, token_list, validation_report
except ImportError:  # pragma: no cover
    from dags.etl_common import (
        default_args,
        input_path,
        output_path,
        token_list,
        validation_report,
    )

SOURCE = "risk_tags.csv"
TARGET = "risk_tags_deduped.csv"
COLUMN = "tags"
DELIM = " "


def dedup_string(value: str, delimiter: str = " ") -> str:
    """Replicate ``%dedup_string``: case-insensitive, order-preserving dedup."""
    seen = set()
    result = []
    for tok in token_list(value, delimiter):
        key = tok.upper()
        if key not in seen:
            seen.add(key)
            result.append(tok)
    return delimiter.join(result)


def dedup_column(df: pd.DataFrame, column: str, delimiter: str = " ") -> pd.DataFrame:
    out = df.copy()
    out[column] = out[column].apply(lambda v: dedup_string(v, delimiter))
    return out


# --- Airflow task callables --------------------------------------------------
def _transform(**_):
    df = pd.read_csv(input_path(SOURCE))
    out = dedup_column(df, COLUMN, DELIM)
    out.to_csv(output_path(TARGET), index=False)


def _validate(**_):
    src = pd.read_csv(input_path(SOURCE))
    out = pd.read_csv(output_path(TARGET))

    no_dupes = True
    subset_ok = True
    for src_val, out_val in zip(src[COLUMN], out[COLUMN]):
        out_tokens = [t.upper() for t in token_list(out_val, DELIM)]
        in_tokens = {t.upper() for t in token_list(src_val, DELIM)}
        if len(out_tokens) != len(set(out_tokens)):
            no_dupes = False
        if not set(out_tokens).issubset(in_tokens):
            subset_ok = False

    validation_report(
        src,
        out,
        expect_equal_rows=True,
        extra_checks={
            "no_duplicate_tokens_in_output": no_dupes,
            "output_tokens_subset_of_input": subset_ok,
        },
    )


# --- DAG ---------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="dedup_string",
        description="SAS dedup_string.sas -> remove duplicate tokens from a string column",
        default_args=default_args(),
        schedule_interval=None,
        catchup=False,
        tags=["sas-migration", "dedup"],
    ) as dag:
        transform = PythonOperator(task_id="dedup", python_callable=_transform)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        transform >> validate
except ImportError:  # pragma: no cover
    dag = None
