"""compare_dag - Airflow port of ``Macro/compare.sas``.

Legacy behavior
---------------
``%compare`` runs PROC COMPARE between a BASE and a COMPARE dataset (or two
libraries). With a BY key it reports:

    * records present in one dataset but not the other, and
    * value differences for the keys present in both,

using a numeric fuzz factor (CRITERION, default 1e-6) and a comparison METHOD.

Port
----
Given base/comp frames and a BY key, produce a tidy difference report:
one row per (key, column) that differs, plus rows flagging keys missing from
either side. Numeric columns are compared with an absolute tolerance
(``criterion``) to mirror the fuzz factor. This DAG's own purpose is
validation, so the ``validate`` task records base/comp row counts and
checksums and asserts the report is internally consistent (difference count ==
report rows, and equal checksums <=> zero differences).
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

BASE = "positions_base.csv"
COMP = "positions_comp.csv"
TARGET = "positions_compare_report.csv"
BY: List[str] = ["account_id"]
CRITERION = 1e-6


def _values_differ(a, b, criterion: float) -> bool:
    a_num = pd.to_numeric(pd.Series([a]), errors="coerce").iloc[0]
    b_num = pd.to_numeric(pd.Series([b]), errors="coerce").iloc[0]
    if pd.notna(a_num) and pd.notna(b_num):
        return abs(a_num - b_num) > criterion
    return str(a) != str(b)


def compare_datasets(
    base: pd.DataFrame,
    comp: pd.DataFrame,
    *,
    by: List[str],
    criterion: float = 1e-6,
) -> pd.DataFrame:
    """Replicate PROC COMPARE (dataset mode) as a tidy difference report."""
    base_keyed = base.set_index(by)
    comp_keyed = comp.set_index(by)
    base_keys = set(base_keyed.index)
    comp_keys = set(comp_keyed.index)

    rows = []
    for key in sorted(base_keys - comp_keys, key=str):
        rows.append({"key": key, "column": "*", "diff_type": "IN_BASE_NOT_COMP",
                     "base_value": "<row>", "comp_value": None})
    for key in sorted(comp_keys - base_keys, key=str):
        rows.append({"key": key, "column": "*", "diff_type": "IN_COMP_NOT_BASE",
                     "base_value": None, "comp_value": "<row>"})

    shared_cols = [c for c in base_keyed.columns if c in comp_keyed.columns]
    for key in sorted(base_keys & comp_keys, key=str):
        for col in shared_cols:
            bv = base_keyed.loc[key, col]
            cv = comp_keyed.loc[key, col]
            if _values_differ(bv, cv, criterion):
                rows.append({"key": key, "column": col, "diff_type": "VALUE_DIFF",
                             "base_value": bv, "comp_value": cv})

    return pd.DataFrame(rows, columns=["key", "column", "diff_type", "base_value", "comp_value"])


# --- Airflow task callables --------------------------------------------------
def _transform(**_):
    base = pd.read_csv(input_path(BASE))
    comp = pd.read_csv(input_path(COMP))
    report = compare_datasets(base, comp, by=BY, criterion=CRITERION)
    report.to_csv(output_path(TARGET), index=False)


def _validate(**_):
    base = pd.read_csv(input_path(BASE))
    comp = pd.read_csv(input_path(COMP))
    report = pd.read_csv(output_path(TARGET))
    expected = compare_datasets(base, comp, by=BY, criterion=CRITERION)

    equal_checksum = dataframe_checksum(base) == dataframe_checksum(comp)
    validation_report(
        base,
        comp,
        extra_checks={
            "report_row_count_equals_difference_count": len(report) == len(expected),
            "checksum_equal_iff_no_differences": (len(expected) == 0) == equal_checksum,
        },
    )


# --- DAG ---------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="compare",
        description="SAS compare.sas -> PROC COMPARE two datasets by key",
        default_args=default_args(),
        schedule_interval=None,
        catchup=False,
        tags=["sas-migration", "compare"],
    ) as dag:
        transform = PythonOperator(task_id="compare", python_callable=_transform)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        transform >> validate
except ImportError:  # pragma: no cover
    dag = None
