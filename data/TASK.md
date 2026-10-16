# DSA5208 Project 2 — Current Status

Updated **13/10/2026**. Individual assignment.
This is the practical checklist for the four current notebooks; `TASK_SPEC.md` retains the detailed specification and older run log for reference. Do not treat planned designs as completed results.

## Completed run

- **Run ID:** `20261012-full-02`
- **Code commit used for the run:** `ea454b3eb1abdb2059dfba283091ca3b80f3c3fe`
- **GCS root:** `gs://sg-rainfall-dsa5208-261011-data/runs/20261012-full-02/`
- **Config SHA-256:** `daeebb8814a3157e3347ecceb152b7e48c47310df1071b7dca08d9622c0d62d6`
- **Manifest SHA-256:** `41080cf58fc68f899c06145d573620b61777bbcc3b13d90c69020a6dcd790688`

### Run with evidence

- [x] Notebook 01: 34 code cells executed, 0 errors, 10 inline figures.
- [x] Notebook 02: 12 code cells executed, 0 errors.
- [x] Notebook 03: 41 code cells executed, 0 errors, 1 inline figure.
- [x] Cleaning completed for 2017–2024: 48,538,713 raw rows; 48,538,627 retained; 86 excluded.
- [x] Task 1 covered all eight years. Annual wet-reading frequency ranged from 1.94% (2019) to 3.20% (2023); the common annual station cohort was empty at coverage thresholds of 90%, 95%, and 99%, so no annual trend was inferred for a fixed network.
- [x] Timestamp distributions were inspected before cleaning. The first block of 2017 used 5-minute timestamps ending at second 59; later 2017 records and 2018–2024 used second 00. The `:59` convention also appears in the official 2016 dataset, but 2016 is outside this analysis. For this run, the **analysis epoch** of 1,775,327 2017 records with a 299-second offset was shifted forward by 1 second to a 5-minute boundary, while preserving the source timestamp. The publisher did not explain the convention change; this was an alignment choice, not confirmation of interval semantics.
- [x] Task 2: train 2017–2022; model selection Jan–Jun 2023; calibration Jul–Dec 2023; 2024 holdout. `gbt_B` was selected on the selection set and isotonic calibration was fitted; the selected model was then compared with climatology on the holdout.
- [x] At the 0.5 mm threshold across 6,374,476 valid prediction times, calibrated `gbt_B` achieved PR-AUC 0.6345 and Brier 0.01952; climatology achieved 0.0732 and 0.03436. F1 at 0.5 is for reference only; the cutoff was not optimized.
- [x] Notebook-generated figures and `full-02` tables/provenance are saved in `report/figures/full-20261012/` and `report/tables/full-20261012/`.
- [x] The model, calibrator, climatology baseline, and selected manifest were downloaded to `artifacts/models/20261012-full-02/` (approximately 2.6 MB).
- [x] A fresh Spark 3.5.3 session loaded the five-stage GBT pipeline, isotonic calibrator, three climatology tables, and selected manifest; a sample 0.5 mm prediction ran successfully. Raw p=0.02881781; calibrated p=0.02589584. This is a serialization/inference smoke check, not a rerun of the holdout. Dataproc job `ee582912264541edb80942445a23994f` reached `DONE`; the one-node cluster `rainfall-reload-20261012` was deleted. Evidence: `report/tables/full-20261012/model_reload_validation.json`.
- [x] The report was updated with this run's measurements and the correct project name, **Singapore Rainfall Analysis and Prediction**, on the cover/metadata; see `report/main.tex`.
- [x] The PDF compiled successfully with `latexmk`, and its layout/tables/figures were reviewed.
- [x] The submission ZIP contains source, notebooks, config/manifest, PDF, tables/figures used in the report, and selected model artifacts; it contains no raw CSVs or workstation files. SHA-256: `58da305466a77c067237ad3a89d30837801296a05bd087a1099c54104c495db6`.

## Remaining before submission

- [ ] **Notebook 00 evidence:** cloud setup ran in the `full-02` workflow, but the local `notebooks/00_setup_cloud.ipynb` does not retain execution outputs. Therefore, do not mark the setup notebook as reproducible by restart-and-run-all.
- [ ] **Submit to Canvas:** check the required format/filename on the assignment page, upload the final package, and confirm the receipt. The repository has no evidence of upload.
- [ ] If cloud work must be rerun, confirm the current project/quota/billing and use a new run prefix; do not reuse `20261012-full-02` as a new run. Use only the credit source specified by the user; do not use a paid fallback.

## Current design

| Notebook | Role | `full-02` run status |
|---|---|---|
| `00_setup_cloud.ipynb` | Prepare data/config and Dataproc | Used; local outputs not saved |
| `01_clean_and_task1.ipynb` | Cleaning and Task 1, all of 2017–2024 | Complete, outputs saved |
| `02_build_features.ipynb` | History features, target, splits | Complete, outputs saved |
| `03_train_evaluate.ipynb` | Tuning, selection, calibration, 2024 test | Complete, outputs saved |

The core scientific work should be immediately accessible in the notebooks. Billing/runtime assurance, source-bundle/version ledger, or screenshot-per-job workflow are not assignment grading requirements. The MLlib model was fitted on a deterministic 2% sample of valid training times; climatology used the full valid training partition. Jan–Jun 2023 is the model-selection set, not an independent generalization estimate. This period was used for both pilot tuning and final candidate comparison, so the selected model's score is likely optimistic and is not an unbiased validation score. The 2024 holdout was not used for post-test tuning.

## Limitations to retain in the report

- Interpreting timestamps as interval end remains an assumption; NEA has not confirmed the timestamp semantics. The early-2017 `:59` block was shifted by +1 second in the analysis epoch to align it with 5-minute boundaries; source timestamps were preserved. The `:59` convention also occurs in official 2016 data, while the publisher has not disclosed why it changed to `:00`.
- The stored dataset is retrospective. The original arrival times of revisions were not reconstructed; do not claim real-time performance.
- Missing current observations are outside the prediction contract. Predictions are made only at valid observation timestamps; labels require all six future readings on the expected grid.
- Annual descriptive summaries use a changing network; the common-station cohort is empty. Do not call between-year differences a climate trend.
- Calibrated GBT's ECE on 2024 is 0.00172, slightly higher than climatology's 0.00142. The calibration trigger/fit does not prove better calibration than the baseline.
- The calibrated GBT monotonicity violation rate is 0.0121% for six primary thresholds and 0.0151% for five intermediate thresholds.

## Submission checks

- [x] Read the compiled PDF and review the cover, tables/figures, references, and measurements.
- [x] Verify that the ZIP matches `20261012-full-02` and contains no raw CSVs or workstation files.
- [x] README/report state that the fresh reload smoke check passed and do not claim that the holdout was rerun.
