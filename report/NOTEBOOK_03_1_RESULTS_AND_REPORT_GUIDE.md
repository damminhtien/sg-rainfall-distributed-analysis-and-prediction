# Notebook 03.1: Verified Results and Report-Writing Guide

**Purpose:** Internal, LLM-facing evidence guide for the DSA5208 Singapore Rainfall Analysis and Prediction report. Not a submission artifact. Numerical results below are taken from the supplied `results.zip` and `monotonic_extension.zip` JSON summaries. Distinguish measured facts from possible explanations; do not invent statistical significance or runtime measurements.

## 1. Source of truth and scope
- Run prefix: `gs://dsa5208-rainfall-project-2-data/runs/20261018-monotonic-01/`.
- Original Notebook 03 evaluation: complete 2024 test set, 6,374,476 station-times × six thresholds = 38,246,856 binary prediction rows.
- Notebook 03.1 monotonic extension: 12,776 sampled 2024 station-times × 13 thresholds = 166,088 prediction rows per method. All three methods use the same sampled station-times and threshold values. Do **not** directly compare numerical scores across the full-holdout and sampled cohorts as though they were the same test population.
- The two ZIPs include all original summary/reliability files and the extension summary, extension score Parquet, extension reliability JSON, fitted multinomial model, global calibrator, and a figure. Original full-holdout `results/scores/` is absent and not required for headline findings.
- The provided archives do not include original fitted GBT artifacts from the separate top-level `models/` folder, nor the complete `base_features/`. Do not claim the repository can reload the full original model using only these ZIPs.

## 2. Research question and method
- Target: strict exceedance probability `P(next 30-minute rainfall > x | station, time, available history)`; for fixed station/time, this must be non-increasing as threshold `x` rises.
- Baseline `gbt_B_calibrated`: original history-feature Spark MLlib gradient-boosted tree with global isotonic probability calibration; does not structurally enforce threshold monotonicity.
- `gbt_B_threshold_isotonic`: baseline probabilities scored on a fixed 26-point correction grid and projected onto a non-increasing sequence using threshold-wise isotonic least squares, with monotonic interpolation. Same underlying fitted GBT; no independent model retraining for this correction.
- `multinomial_B_empirical_cdf`: Spark MLlib multinomial logistic regression predicts probabilities for nine mutually exclusive 30-minute rainfall categories: exactly 0; (0,0.2]; (0.2,0.5]; (0.5,1]; (1,2]; (2,5]; (5,10]; (10,20]; >20 mm. Empirical within-bin CDFs estimated from all valid 2017–2022 targets allocate bin mass for arbitrary thresholds. Nonnegative bin probabilities and nondecreasing within-bin CDFs guarantee a non-increasing exceedance curve on the supported 0–20 mm domain.
- Training split 2017–2022; selection Jan–Jun 2023; calibration Jul–Dec 2023; holdout 2024. Extension uses training sample fraction 0.02, calibration sample fraction 0.01, test sample fraction 0.002; multinomial regularization `0.05`; no post-test tuning recorded.
- The original six training thresholds were [0.2, 0.5, 1, 2, 5, 10] mm. The extension evaluates [0, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 3.5, 5, 7.5, 10, 20] mm. Some are unseen thresholds; `0` and `20` are additional extension points. Avoid describing all 13 as originally trained thresholds.
- Correction grid (mm): 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.75, 0.8, 1.0, 1.2, 1.5, 1.75, 2.0, 2.5, 3.5, 4.0, 5.0, 6.0, 7.5, 8.5, 10.0, 12.5, 15.0, 17.5, 20.0.
- Supported threshold range for both monotonic constructions: 0–20 mm. The corrected GBT numerical predictions depend on grid and interpolation; the multinomial within-bin shape depends on global training-derived CDFs.

## 3. Model-selection and full 2024 holdout (original workflow)
- Selection chose **`gbt_B`** with parameters `{'maxDepth': 4, 'maxIter': 40}`. Selection ECE = 0.022579 exceeded trigger 0.010, so isotonic score calibration was applied. The selection record says the test year was not accessed during selection; the evaluation record says `post_test_tuning = False`.
- Full test evaluation contains 38,246,856 expanded binary rows; the reference classification cutoff is 0.5 and was not tuned.

| Threshold (mm) | Climatology Brier ↓ | Calibrated GBT Brier ↓ | Climatology PR-AUC ↑ | Calibrated GBT PR-AUC ↑ |
|---:|---:|---:|---:|---:|
| 0.2 | 0.045201 | 0.025197 | 0.09861 | 0.66148 |
| 0.5 | 0.034360 | 0.019525 | 0.07320 | 0.63422 |
| 1 | 0.024744 | 0.015184 | 0.05292 | 0.58100 |
| 2 | 0.016420 | 0.010994 | 0.03621 | 0.50655 |
| 5 | 0.008249 | 0.006133 | 0.01934 | 0.39558 |
| 10 | 0.003801 | 0.003086 | 0.00976 | 0.30100 |

**Interpretation:** On the complete 2024 holdout, the calibrated history-feature GBT beats the climatology baseline at every original threshold on both Brier score and PR-AUC. At 0.5 mm, the Brier score reduction is about 43.2%. This is evidence for the predictive value of the learned rainfall-history model relative to climatology, not a causal ablation of individual feature families.

**Original full-holdout monotonicity:** {'threshold_pairs': 25497904, 'violation_rate': 0.00010502824075265167, 'max_increase': 0.3027792294358738}.

## 4. Monotonic extension: matched 2024 sample
- Number of station-times per method: 12,776. Adjacent threshold comparisons per method: 153,312.
- The mean Brier and mean PR-AUC below are **simple unweighted averages over 13 thresholds**, computed from recorded per-threshold metrics. They are not pooled-row estimates and not an official notebook summary metric.

| Method | Mean Brier ↓ | Mean PR-AUC ↑ | ECE ↓ | Violating adjacent pairs | Max probability increase |
|---|---:|---:|---:|---:|---:|
| `gbt_B_calibrated` | 0.015067859 | 0.463474 | 0.004600565 | 2 | 0.01266203 |
| `gbt_B_threshold_isotonic` | 0.015067858 | 0.463471 | 0.004600565 | 0 | 0.00000000 |
| `multinomial_B_empirical_cdf` | 0.015972625 | 0.418461 | 0.003561386 | 0 | 0.00000000 |

**Main result:** The sampled original calibrated GBT has 2 violating adjacent threshold pairs out of 153,312 (violation rate ~1.3045e-5; maximum increase ~0.012662). Both monotonic methods have zero recorded violations and zero maximum increase. Threshold-wise isotonic correction changes mean Brier by approximately -1.08e-9 and mean PR-AUC by approximately -3.60e-6 relative to original GBT: negligible on this sample. Multinomial mean Brier is ~6.0% higher and mean PR-AUC ~9.7% lower than GBT, although aggregate ECE is lower (0.00356 vs 0.00460).

### Threshold-specific comparison (matched sample)
| x (mm) | GBT Brier | Corrected Brier | Multinomial Brier | GBT PR-AUC | Corrected PR-AUC | Multinomial PR-AUC | Positives |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0.033877 | 0.033877 | 0.035596 | 0.6571 | 0.6571 | 0.6353 | 802 |
| 0.2 | 0.025358 | 0.025358 | 0.027243 | 0.6590 | 0.6590 | 0.6278 | 623 |
| 0.3 | 0.024469 | 0.024469 | 0.025785 | 0.6362 | 0.6362 | 0.6024 | 574 |
| 0.5 | 0.020814 | 0.020814 | 0.022425 | 0.6309 | 0.6309 | 0.5888 | 490 |
| 0.75 | 0.018995 | 0.018995 | 0.020498 | 0.6136 | 0.6136 | 0.5508 | 421 |
| 1 | 0.016238 | 0.016239 | 0.017733 | 0.5897 | 0.5896 | 0.5354 | 353 |
| 1.5 | 0.014693 | 0.014693 | 0.015836 | 0.5428 | 0.5427 | 0.4750 | 292 |
| 2 | 0.012029 | 0.012029 | 0.013214 | 0.5137 | 0.5137 | 0.4356 | 234 |
| 3.5 | 0.009810 | 0.009810 | 0.010077 | 0.4335 | 0.4335 | 0.3571 | 165 |
| 5 | 0.007224 | 0.007224 | 0.007588 | 0.3176 | 0.3176 | 0.2656 | 117 |
| 7.5 | 0.006285 | 0.006285 | 0.006192 | 0.2011 | 0.2011 | 0.1684 | 88 |
| 10 | 0.004383 | 0.004383 | 0.004374 | 0.1640 | 0.1640 | 0.1623 | 62 |
| 20 | 0.001707 | 0.001707 | 0.001084 | 0.0661 | 0.0661 | 0.0354 | 14 |

**Interpretation:** GBT-based methods have higher PR-AUC at all 13 thresholds. Multinomial Brier is worse at most light/moderate thresholds, but lower at 7.5, 10, and 20 mm; the high-threshold advantage should not be equated with better rare-event discrimination, because prevalence is extremely low. At 20 mm, only **14 positive observations** occur in the 12,776 station-times, so tail metrics are unstable. The multinomial model predicts more conservatively and may show lower binned ECE despite weaker Brier and PR-AUC. Do not claim statistical significance without uncertainty analysis.

## 5. Discussion points and interpretation boundaries
1. **Mathematical consistency vs predictive skill:** A coherent survival curve must be non-increasing. Both extension methods satisfy this by construction over 0–20 mm; the original unconstrained classifier does not. Zero observed violations on a finite grid alone would not prove a universal guarantee, but the algorithms supply the structural justification.
2. **Isotonic correction is a strong practical result:** On the matched test sample it removed the two observed reversals while barely changing metrics. This does **not** show that isotonic projection always preserves accuracy, nor that the correction has zero inference cost.
3. **Multinomial limitations:** Linear multinomial logistic regression has less flexibility than a nonlinear GBT; its global within-bin empirical CDF cannot vary by station/weather conditions. These are plausible explanations for weaker predictive discrimination, not experimentally isolated causes.
4. **Calibration:** ECE is bin-dependent; a lower ECE alone does not dominate Brier or PR-AUC. Present reliability plots and metrics together.
5. **Sample vs full holdout:** Full-holdout GBT performance and sampled three-method performance have different denominators. The extension sample contains far fewer rare heavy-rain events; do not treat differences in full vs sampled values as model drift.
6. **Threshold precision:** Strict `>` labels, separate zero-rainfall class, nonuniform bins, and empirical within-bin CDF are appropriate given strong zero inflation and concentration of rainfall at discrete values. Bins are experimental choices; do not imply they were mandated by the data publisher.
7. **Scalable computing:** Both trained approaches use Spark MLlib and distributed Spark DataFrames; the isotonic post-processing may require many threshold evaluations per station-time query. The supplied results do not include controlled runtime, cost, memory, or latency benchmarks; do not assert one method is faster.
8. **Validation:** The original holdout model was selected using 2023 data and tested on 2024. The extension methods were compared on a 0.2% 2024 sample without reported post-test tuning. Avoid selecting a new model on the same holdout and then presenting its measured score as an unbiased selection-confirming estimate.
9. **Report limitations:** Single Dataproc deployment, training subsampling, limited 0–20 mm prediction domain, correction-grid dependence, empirical CDF conditional-shape assumption, rare-event instability, no uncertainty intervals, and absence of a measured inference-cost comparison.

## 6. Recommended report figures/tables
- Table A: original complete-holdout climatology vs selected GBT at six original thresholds (Section 3).
- Table B: matched-sample three-method summary with monotonicity violations, mean Brier, mean PR-AUC and ECE (Section 4). Explain equal-threshold averaging.
- Figure A: Brier vs threshold and PR-AUC vs threshold using the exact per-threshold JSON; these can be regenerated locally without the original large scores.
- Figure B: existing `monotonic_extension/figures/monotonic_probability_comparison.png` to illustrate example curves; it is an example, not a population-level performance plot.
- Figure C: reliability diagrams from `monotonic_extension/reliability/<model>/part-*.json`, with caveats about rare-event bins.
- Optional appendix: original full-holdout monotonicity, unseen-threshold checks, selection hyperparameters, empirical bins, and sample sizes.

## 7. Source-file map and how to cite numerical evidence internally
- `results/selection_summary/part-*.txt`: chosen model, hyperparameters, calibration trigger, candidate selection metrics.
- `results/evaluation_summary/part-*.txt`: original full 2024 holdout, climatology vs GBT, unseen thresholds, monotonicity.
- `results/reliability/{climatology,selected_calibrated}/part-*.json`: original reliability bins.
- `monotonic_extension/evaluation_summary/part-*.txt`: **primary source** for all extension model comparisons and parameters.
- `monotonic_extension/reliability/<model>/part-*.json`: reliability data for each of the three extension methods.
- `monotonic_extension/scores/<model>/*.parquet`: matched-sample prediction-level data for optional paired tests, plots, audits. These are small enough to retain as ZIPs or Git LFS if needed, but not required for the primary tables.
- `monotonic_extension/figures/monotonic_probability_comparison.png`: existing figure.
- `monotonic_extension/models/`: fitted multinomial model and global calibrator; keep archived for reproducibility, but they are not required for report writing and should not be committed as many tiny generated files.
- Executed `notebooks/03_1_monotonic_prediction.ipynb`: implementation and saved notebook outputs; ensure report text matches actual executed code.

## 8. Suggested GitHub handoff layout
```text
docs/analysis/
  NOTEBOOK_03_1_MONOTONICITY_DESIGN.md
  NOTEBOOK_03_1_RESULTS_AND_REPORT_GUIDE.md
  results/
    original/selection_summary.json
    original/evaluation_summary.json
    original/reliability_climatology.json
    original/reliability_selected_calibrated.json
    monotonic/extension_evaluation_summary.json
    monotonic/reliability_gbt_calibrated.json
    monotonic/reliability_gbt_threshold_isotonic.json
    monotonic/reliability_multinomial_empirical_cdf.json
report/figures/
  monotonic_probability_comparison.png
notebooks/
  03_1_monotonic_prediction.ipynb
```
Rename the single-part JSON/text files into the descriptive names above **without changing their contents**. The exact original Spark output directory names are also acceptable if renaming is inconvenient. Preserve original run ZIPs outside Git as backups. If score-level reanalysis is desired, store `monotonic_extension/scores/` separately under Git LFS or a downloadable archive; avoid committing the entire Spark `models/` tree or the multi-GB raw and feature data to ordinary Git.

## 9. Claims not supported by these artifacts
- No controlled training/inference time comparison or actual cost comparison.
- No confidence intervals or significance test for differences in PR-AUC/Brier.
- No evidence that multinomial models in general are inferior, only this implementation.
- No guarantee of calibrated probabilities for arbitrary out-of-domain thresholds above 20 mm.
- No proof that every model evaluation can be reproduced without the original dataset, complete model files, and runtime.
- No reason to claim monotonic correction improved predictive accuracy meaningfully: the difference is negligible.
