# Singapore rainfall analysis and prediction

DSA5208 Project 2, individual submission by DAM MINH TIEN (A0355091E).
All project execution occurs in Jupyter notebooks. Shared functions under
`src/rainfall/` and `scripts/` are imported from notebook cells.

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

Run notebooks 01–05 in the Dataproc **Python 3** kernel. They create Spark on
YARN and require event logging. Do not install a replacement PySpark runtime.
Notebook 00 runs on the Mac in a Jupyter kernel. Set `DATA_ROOT` in the local
notebook environment to the directory containing `raw/` before running it.
Local Python dependencies are
in `requirements-local.txt`. There is no local Spark cluster execution path.

## Notebook order

1. `00_environment.ipynb`: exact billing/credit preflight, official price
   lookup, quota/network/identity setup, private bucket, selected inputs,
   source/config upload, bounded-lifetime cluster, and shutdown.
2. `01_data_quality.ipynb`: hash verification, read-only raw audit,
   deterministic cleaning, exclusions, alignment, coverage and Parquet.
3. `02_task1_weather_patterns.ipynb`: training-period patterns and figures;
   full 2017–2024 descriptive analysis is run only after Task 2 is frozen.
4. `03_task2_features.ipynb`: contract fixtures, elapsed-time histories,
   six-interval future labels, split-boundary purging and sample attrition.
5. `04_task2_models.ipynb`: train-only preprocessing, climatology/LR/GBT,
   temporal selection, A/B ablation, separate calibration and immutable freeze.
6. `05_final_evaluation.ipynb`: once-only 2024 evaluation, unseen thresholds,
   reliability/PR curves, monotonicity, error strata and paired weekly Brier
   bootstrap; full Task 1 analysis; model reload and inference evidence.
7. A final report/package notebook is still to be added after the frozen model,
   final evaluation, evidence and submission PDF are available. It must validate
   these artifacts before creating an archive.

Notebook 04 freezes the experiment configuration with its selected model.
Spark action outputs include application and job identifiers. After every
Spark action, open the Spark Web UI Jobs summary and each job's detail page,
capture the original screenshots, and index them under
`report/evidence/spark-ui/`. Use History Server after application termination.

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
of extrapolation. Out-of-domain thresholds and off-grid times return statuses.

Train: 2017–2022; model selection: Jan–Jun 2023; calibration: Jul–Dec 2023;
final test: 2024. Labels cannot end at or after the next subset boundary.
Threshold copies share the original station/time sample and temporal split.
The frozen config records deterministic pilot and final-training fractions.
MLlib sampling preserves natural prevalence; the report must state the actual
fraction, sample count and coverage, rather than claiming full-data training.
Climatology fits all valid training base samples.

## Inference after downloading the model package

In a fresh Spark/Jupyter session, load your own validated history DataFrame
(the full raw dataset is deliberately excluded from the submission archive):

```python
from rainfall.predict import RainfallPredictor
predictor = RainfallPredictor.load(spark, "models/deployment", history)
predictor.predict("S24", "2024-01-01T12:00:00+08:00", 1.0)
```

History requires `station_id`, `epoch`, `rain_mm`, `rain_units`, with unique
exact-grid station/time keys. Prediction is supported only when a non-null
current observation exists at the query time; otherwise the predictor returns
`missing_current_observation` with no probability. It selects history at or
before a supported query time and constructs the same features. The package
includes a small
synthetic history fixture for loading and interface checks. Its probabilities
are interface evidence, not measurements of real rainfall performance.

## Status

Read `data/TASK.md` and the execution status file for measured progress.
A prepared notebook or compiled draft report does not prove that a cloud job,
trained model or final evaluation has completed. Only actual outputs and their
run manifests may be used as report results. Canvas submission is performed by
the student after reviewing the final archive and PDF.
