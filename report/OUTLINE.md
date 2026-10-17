# Report status - Singapore Rainfall Analysis and Prediction

Updated 17 October 2026. main.tex and main.pdf now report protocol v2 run
20261016-v2-fixed8-04. Historical v1 source is preserved in
report/archive/v1/main.tex; its PDF copy remains local at
report/archive/v1/main.pdf and is excluded from Git as a generated file. Do not
use v1 Task 2 measurements as v2 evidence. Since
2024 metrics were already observed in the v1 analysis, v2 calls 2024 the
**held-out evaluation period**, not an untouched blind test.

## V2 report narrative

1. **Data and cleaning.** Retain the measured eight-year cleaning and Task 1
   findings. Preserve the interval-end assumption, early-2017 one-second
   analysis alignment, retrospective-data limitation, and changing-network
   caveat.
2. **Prediction target and features.** Define the strict target
   F_30 > tau, six future readings, observed-time queries, history features,
   and missing-window exclusions.
3. **Temporal design.** Report purged roles: 2017-2020 hyperparameter training,
   2021 hyperparameter validation, 2022 candidate selection, full 2017-2022
   final refit, 2023 calibration, and 2024 final evaluation. Explain that the
   selected 2022 score is subject to model-selection optimism.
4. **Model selection.** Hyperparameter screening uses a deterministic 10%
   sample before survivor refits on all eligible 2017-2020 rows. The 2022
   comparison uses 10% samples of 2017-2021 training and 2022 evaluation.
   Report the five candidates and measured counts/metrics/fit times; LR-B was
   removed. No training-fraction scaling study was run.
5. **Probability pipeline.** Report GBT-B refitted on every valid 2017-2022
   row, predetermined 2023 isotonic calibration, equal-weight non-increasing
   PAVA over six anchors, and in-domain linear interpolation.
6. **Final evaluation.** Compare raw, calibrated and PAVA outputs plus
   full-training climatology on the same 2024 cohort. Include per-threshold
   PR-AUC/Brier/support, ECE, monotonicity/max increase, intermediate threshold
   results and paired weekly-block uncertainty. Add a clearly labeled
   retrospective v1-v2 comparison, preserve v1 rounding, and disclose that both
   use the previously observed 2024 period.
7. **Execution provenance.** State that the sequential runner recorded
   Notebook 03 success but Notebook 04 failure at the original PAVA expression.
   The final evaluation summary came from recovery attempt 03 using saved
   calibrated 2024 anchor predictions; do not claim an error-free Notebook 04
   run-all. Record the stopped 8-worker cluster and retained GCS/local artifacts.
8. **Comparison interpretation.** State that v1 and v2 predictive scores are
   close at most thresholds, v2 is lower at 10 mm, and its main methodological
   changes are full-data fitting, temporal separation and exact monotonicity.
   Holdout ECE remains slightly above climatology.
9. **Limitations.** Do not claim real-time performance or call the previously
   observed 2024 period an untouched blind test. State that no scaling study
   was run.

## Evidence status

- Notebook 03 succeeded, saved candidate results and artifacts/frozen_spec.json;
  selected model: gbt_B.
- Notebook 04 fitted the 100% 2017-2022 model and 2023 calibrator, then failed
  in the original PAVA implementation. Recovery attempt 03 produced the final
  evaluation summary from saved predictions.
- Final anchor PAVA and the five in-domain interpolated thresholds have zero
  monotonicity violations. Raw/calibrated violations are retained for comparison.
- The report now includes a v1-v2 metric table using the shared 2024 cohort,
  preserves the rounding of the archived v1 values, and labels the comparison
  retrospective rather than independent.
- Final model/reliability artifacts, full run source, logs, Parquet predictions
  and manifests are in the local mirror at
  /Users/macbook/Data/sg-rainfall-v2-fixed8-04/; checksum-only rsync found no
  differences from GCS.
- The report compiled to a 15-page A4 PDF. Page thumbnails and the primary
  results/provenance tables were visually inspected.
- A separate inference smoke test of the saved model with an explicit
  out-of-domain rejection is still pending, as are final package checks and
  Canvas upload receipt.
- The Dataproc cluster dsa5208-rainfall-fixed8 is STOPPED. Its GCS outputs
  remain available; the cluster was not deleted.

## Current report artifacts

- Source: report/main.tex
- PDF: report/main.pdf
- Historical v1 source/PDF: report/archive/v1/main.tex (in Git) and
  report/archive/v1/main.pdf (local copy)
- Final metrics: results/recovery/attempt-03/final_evaluation_summary.json
- Original sequential status: results/overnight_execution/status.json
