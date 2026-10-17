# Singapore Rainfall Analysis and Prediction

This repository contains an individual DSA5208 Project 2 analysis of the
National Environment Agency's five-minute rainfall observations in Singapore.
Task 1 describes rainfall patterns from 2017–2024. Task 2 estimates the
probability that the next 30-minute accumulated rainfall exceeds a requested
threshold. The prediction workflow uses Spark for feature construction and
Spark MLlib for model development.

## Task 2 protocol v2

| Years | Role |
|---|---|
| 2017–2020 | Deterministic 10% hyperparameter screen; refit survivors on all valid rows |
| 2021 | Hyperparameter validation |
| 2022 | Model/feature selection on a deterministic 10% sample |
| 2017–2022 | Full-data refit of the frozen winner |
| 2023 | Fit the predetermined isotonic calibrator on a deterministic 10% sample |
| 2024 | Held-out final evaluation after freeze |

The 30-minute label window is purged at each split boundary. Notebook 03 reads
only the 2017–2022 development partitions. It selects the model/feature
candidate on 2022 and writes the selected specification and provenance hashes
to `artifacts/frozen_spec.json`. Notebook 04 verifies that freeze, refits the
selected model on all valid 2017–2022 rows, and always fits isotonic calibration
on 2023. It projects calibrated probabilities onto a non-increasing curve
using equal-weight PAVA, then linearly interpolates for thresholds inside the
configured domain. Queries outside the domain are rejected.

Hyperparameter screening uses a deterministic 10% sample before surviving
configurations are refit on all eligible 2017–2020 rows. Model/feature
selection and calibration each use a deterministic 10% sample of their
designated development partition. The frozen winner is refit on 100% of valid
2017–2022 rows. No training-fraction scaling study is run. The v1 2024 metrics
were previously observed. Therefore, v2 calls 2024 a **held-out
evaluation period**, not an untouched blind test, and does not use v1 metrics
to make v2 decisions.

The completed v2 run is `20261016-v2-fixed8-04`. Notebook 03 succeeded and
selected GBT-B; the original sequential Notebook 04 run failed at PAVA, while
recovery attempt 03 completed the final evaluation from saved 2024 predictions.
The current report describes v2 and includes a retrospective comparison with
v1. The two runs have similar PR-AUC/Brier at most thresholds; v2 enforces
non-increasing probabilities with PAVA. Because v1 2024 metrics were already
observed, this comparison is not a new independent test. See
[`data/TASK.md`](data/TASK.md) for exact metrics, caveats and remaining work.

## Repository layout

| Path | Purpose |
|---|---|
| `config/experiment.json` | Protocol v2 splits, thresholds, features, and model grids |
| `manifests/input_manifest.json` | Eight 2017–2024 inputs, schemas, row counts, and hashes |
| `notebooks/00_setup_cloud.ipynb` | Local input preflight, GCS upload, and Dataproc setup |
| `notebooks/01_clean_and_task1.ipynb` | Data cleaning, coverage checks, and Task 1 |
| `notebooks/02_build_features.ipynb` | History features, target windows, and purged temporal splits |
| `notebooks/03_model_development.ipynb` | Hyperparameter/model selection and freeze |
| `notebooks/04_final_fit_evaluate.ipynb` | Full-data refit, 2023 calibration, PAVA, and 2024 evaluation |
| `scripts/rainfall_modeling.py` | Shared Spark ML, metrics, climatology, PAVA, and interpolation functions |
| `scripts/build_manifest.py` | Reusable input inventory and validation functions |
| `data/TASK.md` | Active v2 tracking and historical v1 evidence |
| `report/OUTLINE.md` | Report status and v2 evidence required |

## Data

Download the annual **Historical Rainfall across Singapore** CSV files for
2017–2024 from the [data.gov.sg collection](https://data.gov.sg/collections/2279/view).
Keep the raw files outside this repository. Set `DATA_ROOT` to a directory
containing a `raw/` folder with the eight manifest-listed files. Raw CSVs are
not tracked in Git or included in the submission package.

The manifest records file inventory, schema, timestamp ranges, row counts, and
SHA-256 digests. It validates the input snapshot but does not certify the
semantic quality of observations. Do not use 2016 as warm-up data.

## Environment

The setup notebook runs locally. Analysis notebooks run in Dataproc Jupyter
with the configured Spark environment; there is no local Spark execution path.
Install Python and the Google Cloud CLI, then authenticate an account with
access to the configured project and bucket:

```bash
gcloud auth login
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-local.txt ipykernel
python -m ipykernel install --user \
  --name dsa5208-rainfall \
  --display-name "Python (DSA5208 Rainfall)"
```

Place the eight CSVs under `$DATA_ROOT/raw/`, set `DATA_ROOT`, and run
`00_setup_cloud.ipynb` locally. Its default run ID is
`20261016-v2-full-01`; use a new run ID for each fresh attempt. Review the
project, Education billing link, region, bucket, quota, and provisioning
settings before running its cloud setup cells. The notebook uploads the
notebooks, shared modeling helper, config, and source hashes for that run.

The analysis notebooks use Dataproc image `2.2.87-debian12` with bundled
Python 3.11 and Spark 3.5.3. Do not install or upgrade PySpark on that image.

## Run the analysis

Keep the same `BUCKET` and `RUN_ID` values in setup and analysis notebooks.
Run in order on the Dataproc Jupyter kernel:

1. `01_clean_and_task1.ipynb` cleans the manifest-listed inputs and produces
   Task 1 summaries and figures.
2. `02_build_features.ipynb` builds history features, labels, and the five
   purged v2 partitions.
3. `03_model_development.ipynb` uses only `search_train`, `hp_validation`, and
   `model_selection`. It creates the frozen spec only after model selection
   and the full-data development model selection complete.
4. `04_final_fit_evaluate.ipynb` refuses to run unless the frozen spec and
   config/input/source hashes match. It refits on all valid 2017–2022 data,
   fits the calibrator on 2023, and then evaluates the held-out 2024 period.

Run outputs are stored under
`gs://<BUCKET>/runs/<RUN_ID>/`. Final scores include raw, calibrated, and
PAVA-projected probabilities at the anchor thresholds. Unseen in-domain
thresholds are answered by linear interpolation; out-of-domain queries fail.

## Cloud cleanup and report

After confirming that notebooks, metrics, model artifacts, and manifests are
persisted, delete the run's Dataproc cluster and verify that no compute
resources remain. Review the remaining storage objects before retention or
cleanup decisions.

The current report is compiled from the measured v2 summary and records the
original Notebook 04 failure and recovery provenance. For a new run, update
results only after its frozen spec and `final_evaluation_summary.json` exist;
compile and visually review the PDF before rebuilding the submission package.
The assignment requires the report, source, and trained model; verify the exact
Canvas upload requirements before submission.
