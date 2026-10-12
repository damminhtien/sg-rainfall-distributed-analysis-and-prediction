# Singapore rainfall analysis and prediction

DSA5208 Project 2, individual submission by DAM MINH TIEN (A0355091E). The
four grader-facing notebooks show the cleaning, Task 1 analysis, feature and
target construction, model selection and final evaluation directly in cells.
Reusable input inventory code is in `scripts/build_manifest.py`.

## Data and setup

Use the eight annual NEA CSVs for 2017–2024 from
[Historical Rainfall across Singapore](https://data.gov.sg/collections/2279/view).
Keep raw files outside the repository under `<DATA_ROOT>/raw/`. The filenames,
row counts and SHA-256 checksums are recorded in
`manifests/input_manifest.json`. The original download time was not recorded;
the manifest scan date is not presented as retrieval time.

Set `DATA_ROOT` in the local environment before running
`notebooks/00_setup_cloud.ipynb`. It prepares the cloud environment; notebooks
01–03 run on the Dataproc Python 3 kernel and use Spark on YARN. The analysis
has no local Spark execution path. Do not install a replacement PySpark into
the Dataproc image. Project-local Python dependencies are listed in
`requirements-local.txt`.

For a new cloud run, use the already designated project/credit arrangement and
check current billing, quota and the exact run prefix first. The completed run
used the authorized Education credit; this README does not claim a current
remaining balance. The cluster list for the designated project was empty
when checked on 12 October 2026. The run-scoped GCS results remain the durable
cloud record.

## Notebook order

1. `00_setup_cloud.ipynb`: prepare local input/config uploads and the cloud
   session.
2. `01_clean_and_task1.ipynb`: clean rainfall records and analyze Task 1 for
   2017–2024. The 2024 descriptive results are not used for Task 2 selection.
3. `02_build_features.ipynb`: construct elapsed-time history features,
   six-reading targets, temporal splits and attrition summaries.
4. `03_train_evaluate.ipynb`: compare climatology, LR, RF and GBT candidates;
   choose on the Jan–Jun 2023 model-selection set, calibrate on Jul–Dec 2023
   and evaluate only the selected model against climatology on the 2024 holdout.
5. `report/main.tex`: explain methods and results from the completed run.

## Completed run

The current report and saved outputs describe run `20261012-full-02`.
Cleaning retained 48,538,627 of 48,538,713 source rows. Task 1 covers all eight
years; annual wet-reading frequency ranged from 1.94% (2019) to 3.20% (2023).
No station met the configured 90%, 95% or 99% annual coverage rule in all
years, so the report does not claim a fixed-network annual trend.

Task 2 selected history-feature `gbt_B` and applied isotonic calibration. On
the 2024 holdout, at the 0.5-mm threshold, it achieved PR-AUC 0.6345 and Brier
score 0.01952, compared with 0.0732 and 0.03436 for the climatology baseline.
These metrics use 6,374,476 valid prediction times. MLlib fitting used a
deterministic 2% sample of valid training times; climatology used all valid
training times. The detailed tables and notebook-generated figures are under
`report/tables/full-20261012/` and `report/figures/full-20261012/`.

The code applied a one-second epoch adjustment to the 1,775,327 2017 records
whose timestamp offset was 299 seconds, while preserving the source timestamp.
This is an analytical assumption, not a timestamp rule confirmed by NEA. The
report describes its effect and the remaining uncertainty.

The saved Spark model, calibrator and climatology artifacts are under
`artifacts/models/20261012-full-02/`. A fresh Spark 3.5.3 session loaded these
artifacts and produced one inference result; the validation cluster was deleted
afterward. This smoke check did not rerun holdout metrics. Notebook 00 was used
in setup, but its local file does not preserve execution outputs; notebooks
01–03 contain successful saved outputs. See `data/TASK.md` for the remaining
Canvas and setup-evidence items.
The current submission bundle is
`submission/DSA5208_Project2_A0355091E_full-20261012-full-02.zip`; verify the
Canvas naming and upload requirements before submitting it.

## Prediction contract and limits

The target is the probability that the sum of six complete future readings is
strictly greater than a threshold. Equality is negative, and missing future
intervals invalidate the label. Prediction is supported at valid observation
timestamps only; missing-current queries are outside the contract. The
interval-end convention is an accepted modelling assumption, not a confirmed
NEA definition. Archived observations make this a retrospective event-time
evaluation; real-time availability and revision history are not claimed.

Train: 2017–2022; model-selection set: Jan–Jun 2023; calibration: Jul–Dec
2023; final test: 2024. The Jan–Jun set was used for both pilot tuning and final
candidate comparison. The selected candidate's score is optimistic, not an
unbiased validation score or an independent generalization estimate. The
2024 holdout was not used for post-test tuning. The classification cutoff was
fixed at 0.5 for descriptive F1 and was not optimized.
