"""Framework-agnostic tests for the SAS-to-Airflow ETL transformation logic.

These exercise the pure transform + validate callables in each DAG module
without requiring an Airflow runtime (the DAG objects are guarded behind an
``ImportError`` fallback when Airflow is not installed).

Run:  python -m pytest tests/test_transforms.py
or:   python tests/test_transforms.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

import compare_dag  # noqa: E402
import dedup_string_dag  # noqa: E402
import etl_common  # noqa: E402
import export_csv_dag  # noqa: E402
import subset_data_dag  # noqa: E402
import transpose_dag  # noqa: E402


def _clean_output():
    for f in [
        export_csv_dag.TARGET,
        subset_data_dag.TARGET,
        transpose_dag.TARGET,
        dedup_string_dag.TARGET,
        compare_dag.TARGET,
    ]:
        p = etl_common.output_path(f)
        if os.path.exists(p):
            os.remove(p)


def test_export_csv():
    export_csv_dag._transform()
    export_csv_dag._validate()
    out = pd.read_csv(etl_common.output_path(export_csv_dag.TARGET))
    src = pd.read_csv(etl_common.input_path(export_csv_dag.SOURCE))
    assert len(out) == len(src)


def test_subset_data():
    subset_data_dag._transform()
    subset_data_dag._validate()
    out = pd.read_csv(etl_common.output_path(subset_data_dag.TARGET))
    assert list(out.columns) == subset_data_dag.KEEP
    assert len(out) == 7  # ACTIVE accounts in the sample data


def test_transpose():
    transpose_dag._transform()
    transpose_dag._validate()
    out = pd.read_csv(etl_common.output_path(transpose_dag.TARGET))
    assert len(out) == 4  # 2 subjects x 2 visits
    assert {"sysbp", "diabp", "pulse"}.issubset(out.columns)


def test_dedup_string():
    dedup_string_dag._transform()
    dedup_string_dag._validate()
    assert dedup_string_dag.dedup_string("C A B B A G E") == "C A B G E"
    assert dedup_string_dag.dedup_string("x|X|y", "|") == "x|y"


def test_compare():
    compare_dag._transform()
    compare_dag._validate()
    report = pd.read_csv(etl_common.output_path(compare_dag.TARGET))
    assert set(report["diff_type"]) <= {"IN_BASE_NOT_COMP", "IN_COMP_NOT_BASE", "VALUE_DIFF"}
    # 1005 only in base, 1006 only in comp, 1002 qty+value differ => 4 diff rows.
    assert len(report) == 4


if __name__ == "__main__":
    _clean_output()
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("PASS", name)
    print("All tests passed.")
