# SAS → Airflow ETL Migration Notes

This document records how the data-transformation macros in the legacy SAS
library (`ts-sas-legacy-analytics/Macro/`) were translated into Apache Airflow
DAGs in this repo (`dags/`). One SAS macro maps to one DAG.

| SAS macro | Airflow DAG (`dag_id`) | Module |
|---|---|---|
| `export_csv.sas` | `export_csv` | `dags/export_csv_dag.py` |
| `subset_data.sas` | `subset_data` | `dags/subset_data_dag.py` |
| `transpose.sas` | `transpose` | `dags/transpose_dag.py` |
| `dedup_string.sas` | `dedup_string` | `dags/dedup_string_dag.py` |
| `compare.sas` | `compare` | `dags/compare_dag.py` |

## Design decisions common to all DAGs

- **Pure transform functions, thin operators.** Each macro's logic lives in a
  plain Python function (`export_csv`, `subset_data`, `transpose`,
  `dedup_string`, `compare_datasets`) that takes/returns pandas DataFrames and
  has no Airflow dependency. The DAG wires those functions into
  `PythonOperator` tasks. This mirrors the SAS split between the reusable macro
  and its calling program, and makes the logic unit-testable without an Airflow
  runtime (`tests/test_transforms.py`).
- **pandas as the DATA-step/PROC engine.** SAS DATA steps and PROCs
  (`PROC TRANSPOSE`, `PROC COMPARE`, `PROC EXPORT`) are replaced by pandas
  operations. A SAS *dataset* becomes a DataFrame; a SAS *library* becomes a
  directory of CSVs.
- **CSV as the interchange format.** SAS datasets don't exist off-platform, so
  sample inputs are CSVs under `dags/data/input/` and task outputs are written
  to `dags/data/output/` (git-ignored). Override the base directory with the
  `AIRFLOW_ETL_DATA_DIR` environment variable. Paths resolve relative to the
  DAGs folder so the DAGs run both on the host and inside the container (where
  `./dags` is mounted at `/opt/airflow/dags`).
- **Every DAG ends in a `validate` task.** Built on
  `etl_common.validation_report`, it records input/output **row counts** and
  content **checksums** and asserts the invariant appropriate to each
  transformation. `dataframe_checksum` canonicalizes a frame (columns sorted,
  rows sorted by full-row string) before hashing, so it is order-independent —
  matching the intent of `PROC COMPARE`, where two datasets with the same
  values are equal regardless of row order. A failed check raises and fails the
  task.
- **`schedule_interval=None`, `catchup=False`.** These are utility/on-demand
  transformations (as the macros were), not time-driven pipelines.
- **Airflow 2.x import path.** `from airflow.operators.python import PythonOperator`.
  The DAG-construction block is guarded by `try/except ImportError` so the
  modules import cleanly for unit tests when Airflow isn't installed.

## Per-macro translation

### 1. `export_csv.sas` → `export_csv`
`%export_csv` is a wrapper over `%export_dlm(dbms=csv)` that writes a dataset to
a comma-delimited flat file.

| SAS option | Airflow port |
|---|---|
| `DATA=` (with embedded WHERE) | `df` argument + optional `where` (pandas `query`) |
| `PATH=` | `dest` file path |
| `REPLACE=` | `replace` flag; when `False` an existing target raises `FileExistsError` (matches SAS `REPLACE=N`) |
| `LABEL=` | `label` flag; renames headers to human labels via a `LABELS` map |
| `HEADER=` | `header` flag on `DataFrame.to_csv` |
| `LRECL=` | **Dropped** — logical record length is a SAS flat-file concept with no pandas equivalent |

**Validation:** output row count equals input row count and the round-tripped
content checksum is unchanged (a lossless export).

### 2. `subset_data.sas` → `subset_data`
Row/column subsetting with rename.

| SAS option | Airflow port |
|---|---|
| `RENAME=` | applied **first** (as in the macro) via `DataFrame.rename`; later options use new names |
| `OBS=` (`"1-5 or 11-15"`) | `_parse_obs` converts 1-based, non-contiguous ranges to 0-based indices (`iloc`) |
| `FIRSTOBS=`/`LASTOBS=` | positional `iloc` slice (1-based inclusive) |
| `WHERE=`/`IF=` | pandas `query` (WHERE and subsetting-IF are semantically equivalent here) |
| `KEEP=`/`DROP=` | column projection |

**Validation:** output never larger than input; row count equals the WHERE
filter's cardinality; checksum of the kept columns matches the input restricted
to the same rows/columns (subset invariant).

### 3. `transpose.sas` → `transpose`
`PROC TRANSPOSE`, long → wide.

| SAS option | Airflow port |
|---|---|
| `BY=` | pivot index (retained key columns; one output row per group) |
| `ID=` | `columns=` — its values name the output columns |
| `VAR=` | `values=` — the transposed cell values |
| `PREFIX=` | prefix applied to generated column names |
| `SORT=`/`NOTSORTED=` | not needed — `pivot_table` groups by key regardless of input order |

Implemented with `pivot_table(aggfunc="first")`. A helper `untranspose`
(wide → long via `melt`) is used only for validation.

**Validation:** output has one row per distinct BY group; **no values lost**
(count of non-null transposed cells equals input row count); un-pivoting the
output reproduces the input's `(by…, id, value)` checksum.

### 4. `dedup_string.sas` → `dedup_string`
Removes duplicate tokens from a delimited string, order-preserving and
case-insensitive (`indexw(upcase(...))` in the macro), keeping original casing.

| SAS param | Airflow port |
|---|---|
| `INVAR=` | source column |
| `OUTVAR=` | target column (defaults to in-place, as in the macro) |
| `DLM=` | delimiter (default space) |

The macro runs per-observation inside a DATA step; the DAG applies the scalar
`dedup_string` function across a column.

**Validation:** row count unchanged (per-observation op); no output string
contains a duplicate token; every output token existed in its input string
(subset invariant).

### 5. `compare.sas` → `compare`
`PROC COMPARE` between a BASE and COMPARE dataset by key.

| SAS option | Airflow port |
|---|---|
| `BASE=`/`COMP=` | two input CSVs → DataFrames |
| `BY=` | key columns; keys present on only one side are reported |
| `CRITERION=` (fuzz, default 1e-6) | absolute numeric tolerance in `_values_differ` |
| `METHOD=` | only the default `EXACT`/absolute-tolerance path is ported |
| Library-vs-library mode | **Not ported** — only the dataset-vs-dataset mode is migrated; a directory of CSVs would be looped externally |

The `PROC COMPARE` report (records-not-in-both + value differences) becomes a
tidy difference report DataFrame: one row per `(key, column)` diff plus rows
flagging keys missing from either side (`diff_type` ∈
`IN_BASE_NOT_COMP`, `IN_COMP_NOT_BASE`, `VALUE_DIFF`).

**Validation:** the compare macro is itself a validation tool, so the
`validate` task records base/comp row counts and checksums and asserts internal
consistency — the persisted report row count equals the recomputed difference
count, and *equal base/comp checksums ⇔ zero differences*.

## Running

```bash
# Unit-test the transformation + validation logic (no Airflow required):
python tests/test_transforms.py          # or: python -m pytest tests/

# Under Airflow (DAGs auto-discovered from ./dags):
docker-compose -f docker-compose.yml up -d
# then trigger any of: export_csv, subset_data, transpose, dedup_string, compare
```

Each DAG produces its output CSV in `dags/data/output/` and then runs the
`validate` task, which prints a JSON report of row counts, checksums, and check
results and fails if any invariant is violated.

## Deliberate scope limitations

- Legacy SAS parameter-validation plumbing (`parmv`, `loop`, `seplist`,
  `kill`) is **not** reimplemented; Python argument handling and pandas cover
  those concerns.
- `export_csv`'s `LRECL`, `transpose`'s multi-`VAR`/`IDLABEL`/`COPY` options,
  and `compare`'s library mode and non-EXACT methods are out of scope for these
  representative ports and are noted above where relevant.
