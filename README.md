# Singapore Rainfall Analysis and Prediction

This repository contains an individual DSA5208 Project 2 analysis of the
National Environment Agency's five-minute rainfall observations in Singapore.
It covers the years 2017–2024 and uses Apache Spark for the data processing
and prediction tasks.

## Project overview

The project addresses two tasks:

1. Describe rainfall patterns within each year and across 2017–2024, including
   data completeness and station coverage.
2. Estimate the probability that a station's total rainfall over the next
   30 minutes exceeds a selected threshold. The target sums the next six
   five-minute readings; incomplete future windows are excluded.

The prediction workflow builds features from the current reading and available
history, compares Spark MLlib models with a climatology baseline, and evaluates
the selected model on a final temporal holdout. Evaluation uses archived data
and is retrospective; it does not claim real-time data availability.

## Repository layout

| Path | Purpose |
| --- | --- |
| `config/experiment.json` | Years, thresholds, feature settings, temporal splits, and model configuration |
| `manifests/input_manifest.json` | Selected input files, schema, row counts, timestamps, and SHA-256 checksums |
| `notebooks/00_setup_cloud.ipynb` | Local data preflight and Google Cloud setup |
| `notebooks/01_clean_and_task1.ipynb` | Data cleaning, coverage checks, and Task 1 analysis |
| `notebooks/02_build_features.ipynb` | Prediction features, labels, and temporal splits |
| `notebooks/03_train_evaluate.ipynb` | Baseline/model comparison, calibration, and final evaluation |
| `scripts/build_manifest.py` | Reusable input inventory and validation functions |
| `requirements-local.txt` | Dependencies for local setup and notebook work |
| `requirements-cluster.txt` | Additional plotting dependency for the Dataproc image |
| `report/` | Report source, PDF, figures, and tables |

## Data

Download the annual **Historical Rainfall across Singapore** CSV files for
2017–2024 from the [data.gov.sg collection](https://data.gov.sg/collections/2279/view).
Keep the raw files outside this repository. Set `DATA_ROOT` to a directory
containing a `raw/` folder with these files:

```text
$DATA_ROOT/
└── raw/
    ├── HistoricalRainfallacrossSingapore2017.csv
    ├── HistoricalRainfallacrossSingapore2018.csv
    ├── ...
    └── HistoricalRainfallacrossSingapore2024.csv
```

The committed manifest selects exactly those eight years and records their
expected schemas, row counts, timestamp ranges, and checksums. The raw CSVs are
not included in Git or uploaded with this source repository. The manifest checks
file inventory and parseability; it does not certify the semantic quality of
the observations.

If you intentionally use a different download, set `REBUILD_MANIFEST = True`
in the manifest cell of `00_setup_cloud.ipynb`, run that cell, and review the
resulting manifest diff before continuing. This updates the tracked manifest.

## Environment setup

The setup notebook runs locally. The three analysis notebooks require the
Dataproc-provided Spark environment; there is no local Spark execution path.

### Local environment

Install Python and the Google Cloud CLI, then authenticate an account that can
access the project and its billing configuration:

```bash
gcloud auth login
```

From the repository root, create a local environment and install the notebook
dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-local.txt ipykernel
python -m ipykernel install --user \
  --name dsa5208-rainfall \
  --display-name "Python (DSA5208 Rainfall)"
```

Place the eight CSVs under a directory's `raw/` folder. Set `DATA_ROOT` to that
directory, then launch JupyterLab from the repository root so the setup
notebook can find the project files:

```bash
export DATA_ROOT="$HOME/data/sg-rainfall"
mkdir -p "$DATA_ROOT/raw"
jupyter lab
```

Select the `Python (DSA5208 Rainfall)` kernel for `00_setup_cloud.ipynb`.

### Google Cloud and Dataproc

Before running `00_setup_cloud.ipynb`, review the project, region, bucket,
`RUN_ID`, billing configuration, and provisioning flags near the top of the
notebook. Its setup cells can create or update Google Cloud resources, upload
the input data and notebooks, and create a Dataproc cluster. Confirm the active
`gcloud` identity, billing linkage, permissions, quotas, and budget before
running those cells. Use a new, unique `RUN_ID` for each fresh run; the setup
notebook refuses to replace a run's existing input manifest.

The analysis notebooks use the Dataproc 2.2.87 Debian 12 image with its bundled
Python 3.11 and Spark 3.5.3. Open the Dataproc Jupyter URL shown by the setup
notebook and run notebooks 01–03 there. Do not install or upgrade PySpark on
the Dataproc image; `requirements-cluster.txt` lists only the additional
plotting package used by the project.

## Run the analysis

Keep the same `BUCKET` and `RUN_ID` values in the setup notebook and notebooks
01–03. When starting a new run, update those values in each notebook before
uploading and running it.

1. **Prepare the run locally.** Open `00_setup_cloud.ipynb` with the local
   kernel. Confirm `DATA_ROOT` and the eight expected files. Leave
   `REBUILD_MANIFEST = False` when using the committed input snapshot. Review
   the cloud settings and provisioning flags, then run the setup notebook from
   top to bottom.
2. **Clean data and complete Task 1.** In Dataproc Jupyter, run
   `01_clean_and_task1.ipynb`. It reads the manifest-listed CSVs, applies the
   documented cleaning rules, and produces rainfall summaries and figures.
3. **Build prediction data.** Run `02_build_features.ipynb` to construct
   historical features, six-reading targets, validity checks, and temporal
   splits.
4. **Train and evaluate.** Run `03_train_evaluate.ipynb` from the first cell
   through the last. It compares the configured baselines and candidates, uses
   2017–2022 for training, Jan–Jun 2023 for selection, Jul–Dec 2023 for
   calibration, and 2024 as the final holdout. The final evaluation depends on
   the earlier selection and calibration cells.

The notebooks persist run outputs under
`gs://<BUCKET>/runs/<RUN_ID>/`. The report source and rendered PDF are under
`report/`.

## Clean up cloud resources

After confirming that outputs and notebooks have been saved, set
`DELETE_CLUSTER = True` in the cleanup cell of `00_setup_cloud.ipynb` and run
that cell to delete the Dataproc cluster. The cluster also has idle-time and
maximum-age deletion settings, but the storage bucket and other supporting
resources remain until separately removed.
