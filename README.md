# Singapore rainfall analysis and prediction

DSA5208 Project 2, individual submission by DAM MINH TIEN (A0355091E).
The grader-facing workflow is four Jupyter notebooks followed by the report.
Cleaning, Task 1 analysis, feature construction, model selection and evaluation
are shown in notebook cells. `scripts/build_manifest.py` is the only reusable
input-inventory helper.

## Data and runtime

Use the eight NEA annual CSVs for 2017–2024 from
[Historical Rainfall across Singapore](https://data.gov.sg/collections/2279/view).
Keep raw files outside the repository in `<DATA_ROOT>/raw/`. Filenames and
SHA-256 checksums are recorded in `manifests/input_manifest.json`.
No raw records, credentials or account tokens are included in this repository.
The original download date was not recorded; the manifest scan date is not
presented as the retrieval date.

The designated cloud project is `sg-rainfall-dsa5208-261011`, region
`asia-southeast1`. Only Billing Account for Education (`01DAD8-AE5F1A-E3910A`)
and credit `3LU63BPGY77JFDQQ` are authorised. Verify the credit on the Console
before a paid session. The USD 40 operating allocation reserves USD 10 from the
original USD 50 credit; budget alerts are not spending caps.

Run notebooks 01–03 in the Dataproc **Python 3** kernel. They create Spark on
YARN; Notebook 00 prepares the cloud environment. Do not install a replacement
PySpark runtime. Notebook 00 runs on the Mac in a Jupyter kernel. Set
`DATA_ROOT` in the local notebook environment to the directory containing
`raw/` before running it. Local Python dependencies are in
`requirements-local.txt`. There is no local Spark cluster execution path.

## Notebook order

1. `00_setup_cloud.ipynb`: prepare the selected cloud project, data bucket and
   Dataproc session.
2. `01_clean_and_task1.ipynb`: read the annual CSVs, clean and align rainfall
   observations, report data quality and Task 1 patterns for 2017–2024. The
   2024 descriptive summaries are not used for Task 2 model selection.
3. `02_build_features.ipynb`: build elapsed-time history features, six-reading
   targets, temporal splits and sample attrition.
4. `03_train_evaluate.ipynb`: compare a station/month/hour climatology
   baseline with logistic regression (LR), random forest (RF) and gradient
   boosted trees (GBT). The selection table contains climatology, LR-A/B, RF-B
   and GBT-A/B; pilot tuning uses four LR, three RF and four GBT configurations.
   Select among climatology, LR-B, RF-B and GBT-B on 2023; calibrate only on the
   separate calibration period; evaluate the selected winner against the baseline
   on 2024. PR-AUC and Brier guide selection, while F1 at 0.5 is descriptive.
   Report reliability, unseen thresholds, monotonicity and paired weekly Brier
   uncertainty in this notebook.
5. `report/main.tex`: summarize the measured notebook results.

Archived experiments, auxiliary cloud diagnostics and Spark UI evidence
notebooks are not part of this analysis path. Source bundles, runtime ledgers
and per-job screenshot capture are not prerequisites for running the four
notebooks.

## Declared data and model assumptions

The user accepted the **interval-end convention**: a reading at `u` represents
`(u-5 min,u]`. This is a modelling assumption, not a confirmed NEA field
specification. Only exact-grid timestamps are retained; no rounding or gap
interpolation is used. Archived final readings imply **retrospective event-time
prediction**, with instantaneous availability assumed. Real-time performance
and reconstruction of revision histories are not claimed.

The target is `P(sum of six complete future readings > threshold_mm)`. Equality
is negative. Missing intervals invalidate labels. Rainfall uses integer
millionths of a millimetre for equality checks. The numeric threshold domain is
0.2–10 mm; intermediate thresholds are evaluated separately, with no promise
of extrapolation.

Train: 2017–2022; model selection: Jan–Jun 2023; calibration: Jul–Dec 2023;
final test: 2024. Labels cannot end at or after the next subset boundary.
Threshold copies share the original station/time sample and temporal split.
The notebook config records deterministic pilot and final-training fractions.
MLlib sampling preserves natural prevalence; the report must state the actual
fraction and sample count rather than claiming full-data training. The
climatology baseline is fit on all valid 2017–2022 training samples.

## Results and report

A prepared notebook or draft report is not evidence that a Spark run or final
evaluation completed. Use metrics and plots produced by Notebook 03 when writing
results; keep claims within the observed years and the retrospective
availability assumption. The student reviews and submits the report.
