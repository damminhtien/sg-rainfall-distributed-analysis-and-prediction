# DSA5208 Project 2 - Active v2 tracker

Updated **17/10/2026**. Protocol v2 replaces the v1 Task 2 pipeline. Run
20261016-v2-fixed8-04 has final evaluation artifacts. Preserve an important
execution distinction: Notebook 03 succeeded, while the original sequential
Notebook 04 returned non-zero at PAVA. A separate recovery job completed the
final metrics from saved model, calibration and 2024 anchor-prediction
artifacts. Do not describe the original sequential run as a clean run-all.

## Protocol v2

| Data period | Use |
|---|---|
| 2017-2020 | Deterministic 10% hyperparameter screening; refit survivors on all valid rows |
| 2021 | Hyperparameter validation |
| 2022 | Candidate selection on a deterministic 10% sample |
| 2017-2022 | Full 100% refit of the frozen winner |
| 2023 | Predetermined isotonic calibration on a deterministic 10% sample |
| 2024 | Held-out final evaluation after freeze |

The 30-minute target is purged at every split boundary. Notebook 03 opens only
search_train, hp_validation and model_selection. Five selection candidates
were retained: climatology, LR-A, RF-B, GBT-A and GBT-B; LR-B was removed.
The 2022 selected GBT-B specification is depth 4 and 40 iterations. No
2/10/50/100% scaling study was run; the final model was fit on 100% of valid
2017-2022 training windows.

Notebook 04 fit the frozen winner on all 34,541,470 valid training windows,
fit isotonic calibration on 10% of 2023, then stopped at the original PAVA
expression. Recovery attempt 03 reused saved calibrated 2024 anchor
predictions, applied the corrected PAVA/interpolation helper, and produced the
final evaluation summary and paired weekly bootstrap. Its output records
post_test_tuning=false; historical v1 2024 metrics were not used for v2
choices. Continue to call 2024 a **held-out evaluation period**, not an
untouched blind test.

## Work tracking

- [x] Record temporal roles, full-data final fit, fixed calibration, PAVA and interpolation.
- [x] Remove pilot_fraction and final_train_fraction; set final training to 100%.
- [x] Update Notebook 02 temporal roles and purged-boundary fixture.
- [x] Replace Notebook 03 design with development-only selection and add Notebook 04.
- [x] Remove LR-B and omit the training-fraction scaling study per the later instruction.
- [x] Run Notebook 03; it succeeded, selected GBT-B from five candidates, and saved the frozen spec.
- [x] Fit the final model on all valid 2017-2022 rows and fit the 2023 isotonic calibrator.
- [x] Recover final 2024 metrics from saved anchor predictions; PAVA and interpolated curves have zero violations.
- [x] Download the full fixed8-04 run prefix and verify the local copy against GCS checksums.
- [x] Update the v2 report, compile it, and visually inspect the rendered PDF.
- [x] Stop the 8-worker Dataproc cluster after confirming no active jobs. Cluster is STOPPED; it was not deleted.
- [ ] Run a separate saved-model inference check for an in-domain threshold and explicit out-of-domain rejection.
- [ ] Build the final submission package, validate it against Canvas requirements, and retain the upload receipt.

## Current cloud and local evidence (17/10/2026)

- **Run:** 20261016-v2-fixed8-04; project sg-rainfall-dsa5208-261011;
  bucket sg-rainfall-dsa5208-261011-data; region asia-southeast1.
- **Notebook 03:** runner status succeeded, return code 0. Five candidates
  were compared using 10% selection training/evaluation samples: 2,742,964
  training windows and 711,112 2022 evaluation windows. GBT-B won with mean
  PR-AUC 0.52207 and mean Brier 0.014016. Frozen spec SHA-256:
  5a7800baac3883a68ad2ed87edb2010aa2f3f82881b8b6537f2a6b2e2aea5743.
- **Notebook 04 original run:** runner status failed, return code 1. It fit
  GBT-B on 34,541,470 base windows (207,248,820 expanded rows), fit an
  isotonic calibrator from 671,768 sampled 2023 windows, then failed because
  PySpark least received one column during PAVA.
- **Recovery:** Dataproc job be25604dcb9941219ffdb120766c65ea is DONE
  (YARN application application_1792163995604_0005 FINISHED). Recovery attempt
  03 reused saved calibrated anchor predictions and did not rerun scoring or
  calibrator transformation. It wrote
  results/recovery/attempt-03/final_evaluation_summary.json.
- **2024 result:** 6,374,476 valid base windows. Final PAVA violation rate
  0.000000 and maximum increase 0 across 31,872,380 anchor pairs. At 0.5 mm,
  final PR-AUC/Brier are 0.63448/0.019581; climatology is 0.07322/0.034360.
  Final-minus-climatology paired weekly Brier difference is -0.008772, with
  95% bootstrap interval [-0.010906, -0.006693] over 53 weeks.
- **Calibration:** 10% of the 2023 partition (671,768 of 6,719,274 base rows).
  The calibration-sample ECE diagnostic fell from 0.0226365 to 0.0016473; it
  is explicitly in-sample.
- **Artifacts:** complete run prefix is 2.67 GiB, 526 files and 2,865,531,461
  bytes, under
  /Users/macbook/Data/sg-rainfall-v2-fixed8-04/. A checksum-only GCS-to-local
  rsync dry run found no differences. This includes the base-feature Parquet,
  models, calibrator, score and reliability Parquet, source notebooks, logs,
  recovery code, summaries and manifests.
- **Report:** report/main.tex and report/main.pdf now describe v2.
  Historical v1 source is preserved in report/archive/v1/main.tex; its PDF
  copy remains local at report/archive/v1/main.pdf. The new PDF
  compiled to 15 A4 pages and was visually reviewed.
- **Cluster:** dsa5208-rainfall-fixed8 has eight e2-highmem-2 workers and is
  STOPPED since 2026-10-17 03:07 UTC. No ACTIVE jobs remained before stop.
  GCS run outputs and local copy are retained; cluster deletion was not done.
- **Frozen hashes:** experiment config
  96076cf5dc04e9c80ed2451be0ea36d2eb6e94b372b91dcce8d849865ad4265c;
  input manifest
  41080cf58fc68f899c06145d573620b61777bbcc3b13d90c69020a6dcd790688;
  source hashes
  6aa04133158626f4c089dd5f607c852d5fec1272542147fdc3fa69576da488cd.
- **Known completion boundary:** the recovery verified synthetic PAVA and
  interpolation behavior and read the saved pipeline for station labels. A
  separate prediction-time model reload test, out-of-domain API test, final
  submission package, and Canvas upload receipt remain outstanding.

## Historical v1 run (not v2 evidence)

- **Run ID:** `20261012-full-02`
- **Code commit:** `ea454b3eb1abdb2059dfba283091ca3b80f3c3fe`
- **GCS root:** `gs://sg-rainfall-dsa5208-261011-data/runs/20261012-full-02/`
- **Config SHA-256:** `daeebb8814a3157e3347ecceb152b7e48c47310df1071b7dca08d9622c0d62d6`
- **Manifest SHA-256:** `41080cf58fc68f899c06145d573620b61777bbcc3b13d90c69020a6dcd790688`
- Cleaning retained 48,538,627 of 48,538,713 rows; 86 were excluded. Task 1 covered 2017–2024.
- The v1 ML models used a deterministic 2% sample while climatology used all training rows. Model selection used Jan–Jun 2023 for both pilot tuning and candidate comparison; calibration used Jul–Dec 2023.
- V1 selected `gbt_B`. At 0.5 mm and 6,374,476 valid 2024 prediction times, calibrated GBT had PR-AUC 0.6345 and Brier 0.01952; climatology had 0.0732 and 0.03436. These metrics were previously observed and must not influence v2 decisions.
- V1's calibrated predictor had recorded monotonicity violation rates of 0.0121% at six primary thresholds and 0.0151% at five intermediate thresholds. V2 must report raw/calibrated/final violations and the constrained output must be strictly non-increasing.
- The v1 model reload smoke check passed. It did not rerun the holdout. The v1 report, PDF, model files, and submission ZIP remain historical artifacts.

## Limitations to retain

- Timestamp-as-interval-end is an assumption not confirmed by NEA. The early-2017 `:59` analysis epoch shift was +1 second; source timestamps were preserved.
- The archive is retrospective; original revision arrival times were not reconstructed, so do not claim real-time performance.
- Predictions require a valid current observation and labels require all six future readings.
- The common annual station cohort was empty at the reported coverage cutoffs; do not interpret changing-network differences as a climate trend.
- V1 2024 metrics have been seen. Call 2024 the **held-out evaluation period** for v2, not an untouched blind test.
