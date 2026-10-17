# Singapore Rainfall Analysis and Prediction

This repository contains a DSA5208 Project 2 analysis of the
National Environment Agency's five-minute rainfall observations in Singapore.
Task 1 describes rainfall patterns from 2017–2024. Task 2 estimates the
probability that the next 30-minute accumulated rainfall exceeds a requested
threshold. The prediction workflow uses Spark for feature construction and
Spark MLlib for model development.

## Task 2 workflow

| Years | Role |
|---|---|
| 2017–2020 | Deterministic 10% hyperparameter screen; refit candidates on valid rows |
| 2021 | Hyperparameter validation |
| 2022 | Model and feature selection on a deterministic 10% sample |
| 2017–2022 | Refit the selected model on all valid windows |
| 2023 | Fit isotonic calibration on a deterministic 10% sample |
| 2024 | Final evaluation period |

The 30-minute label uses six exact five-minute observations after each
prediction time; windows that cross a split boundary are excluded. Notebook 03
performs hyperparameter and model/feature selection. Notebook 04 refits the
selected model on valid 2017–2022 rows, fits isotonic calibration on 2023, then
evaluates on 2024. The prediction step applies equal-weight PAVA to the six
anchor thresholds and linearly interpolates between them for in-domain
queries. Climatology probabilities are estimated from training data only.

Hyperparameter screening uses a deterministic 10% sample from 2017–2020, with
surviving configurations refit on that period's valid rows. Hyperparameter
validation uses 2021; model/feature selection and calibration use deterministic
10% samples of their respective partitions.

During final evaluation, a PySpark expression error in the PAVA
post-processing step was corrected, and the affected post-processing metrics
were recomputed from the saved model predictions; model fitting and calibration
were unchanged.

## Repository layout

| Path | Purpose |
|---|---|
| `config/experiment.json` | Split periods, thresholds, features, and model grids |
| `manifests/input_manifest.json` | Eight 2017–2024 inputs, schemas, row counts, and hashes |
| `notebooks/00_setup_cloud.ipynb` | Local input preflight, GCS upload, and Dataproc setup |
| `notebooks/01_clean_and_task1.ipynb` | Data cleaning, coverage checks, and Task 1 |
| `notebooks/02_build_features.ipynb` | History features, target windows, and purged temporal splits |
| `notebooks/03_model_development.ipynb` | Hyperparameter and model selection |
| `notebooks/04_final_fit_evaluate.ipynb` | Full-data refit, 2023 calibration, PAVA, and 2024 evaluation |
| `scripts/rainfall_modeling.py` | Shared Spark ML, metrics, climatology, PAVA, and interpolation functions |
| `scripts/build_manifest.py` | Reusable input inventory and validation functions |
| `scripts/run_notebooks_sequentially.py` | Sequential Dataproc runner for Notebooks 03 and 04 |

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
`00_setup_cloud.ipynb` locally. Its default target run ID is
`20261016-v2-fixed8-04`; use a new target run ID for each fresh model run. Review the
project, Education billing link, region, bucket, quota, and provisioning
settings before running its cloud setup cells. The notebook uploads the
notebooks, shared modeling helper, config, and source hashes for that run. It
copies the existing base-feature partitions from the run named by
`base_features_source_run_id` in the config, currently
`20261016-v2-full-01`.

The analysis notebooks use Dataproc image `2.2.87-debian12` with bundled
Python 3.11 and Spark 3.5.3. Do not install or upgrade PySpark on that image.

## Run the analysis

Keep `BUCKET` consistent. Notebook 00 creates the target run and copies the
feature partitions named by `base_features_source_run_id` in the experiment
config. Notebook 03 and Notebook 04 use the target run ID.

1. Run `00_setup_cloud.ipynb` locally to upload inputs and source files,
   create or check the Dataproc cluster, and copy the saved feature partitions
   into the target run.
2. `03_model_development.ipynb` uses only `search_train`, `hp_validation`, and
   `model_selection`. It writes the selected model specification after
   candidate comparison.
3. `04_final_fit_evaluate.ipynb` checks the selected specification and
   config/input/source hashes before it refits on all valid 2017–2022 data,
   fits the calibrator on 2023, and then evaluates the held-out 2024 period.

On the Dataproc master, the same two stages can run sequentially with
`RUN_ID=<target-run-id> python scripts/run_notebooks_sequentially.py`. Start an
existing STOPPED cluster before launching Spark jobs.

Notebooks 01 and 02 clean the raw inputs and build the saved feature
partitions. Run them when those partitions need to be regenerated.

Run outputs are stored under
`gs://<BUCKET>/runs/<RUN_ID>/`. Final scores include raw, calibrated, and
PAVA-projected probabilities at the anchor thresholds. Unseen in-domain
thresholds are answered by linear interpolation; out-of-domain queries fail.

## Cloud cleanup and report

After confirming that notebooks, metrics, model artifacts, and manifests are
persisted, delete the run's Dataproc cluster and verify that no compute
resources remain. Review the remaining storage objects before retention or
cleanup decisions.

The report presents Task 1 findings and measured Task 2 evaluation results.
For a new run, update the reported results after
`final_evaluation_summary.json` is written, then compile and visually review
the PDF before rebuilding the submission package.
The assignment requires the report, source, and trained model; verify the exact
Canvas upload requirements before submission.
