# Notebook 03.1: Monotonic rainfall probability extension

**Status:** Design and implementation notes for internal report drafting. Not a results report. The notebook has not been validated on a new Dataproc run. Do not describe expected outcomes as measured findings.

**Source of truth:** `notebooks/03_1_monotonic_prediction.ipynb`, particularly the appended section headed *Monotonic probability extension — fixed methods*. This document explains the rationale and interpretation; the notebook defines the precise executable behavior.

## 1. Purpose and relationship to Notebook 03

The course asks for a probability that the next 30-minute rainfall total at a specified station and time **strictly exceeds** a user-specified threshold. Write the target as `P(Y > x | z)`, where `Y` is the next 30-minute accumulation and `z` contains the station, prediction time, and available historical features. For the same `z`, these probabilities must be non-increasing as `x` increases, since `{Y > x2}` is a subset of `{Y > x1}` whenever `x1 < x2`.

Notebook 03 uses a threshold-expanded binary classification formulation: each station-time example is replicated at a set of thresholds and assigned the label `1[Y > x]`. It compares climatology, MLlib logistic regression, random forest, and gradient-boosted trees (GBT), uses 2023 for selection/calibration, and evaluates 2024. In this formulation, `threshold_mm` is an ordinary feature. The GBT can fit good probability estimates but is not mathematically constrained to decrease with the threshold. The notebook's existing isotonic *score calibration* is a monotone mapping from model score to probability, **not** an enforcement of monotonicity with respect to rainfall threshold.

Notebook 03.1 **retains the original Notebook 03 workflow** (including its existing training, model selection, calibration, full-holdout evaluation, metrics and figures) and appends a separate comparison of two native-MLlib-compatible monotonic strategies. It is intended to replace Notebook 03 in a fresh run after Notebooks 00, 01 and 02, not to be run after Notebook 03. The extension has a separate output prefix `.../monotonic_extension` under the same `RUN_ID`.

## 2. Why these two strategies

### Method A: GBT-B with threshold-wise isotonic projection

The extension uses the fitted `gbt_B` pipeline (even if the original model-selection step chose a different finalist). It fits a **separate global isotonic score calibrator** using a deterministic 1% sample of the July–December 2023 calibration split. The score calibrator is non-decreasing in the raw score and is shared across all thresholds. Next, for each station-time input, it scores a fixed threshold grid, then uses the pool-adjacent-violators (PAV) algorithm to find the **equal-weight least-squares closest non-increasing sequence** of probabilities. The fitted sequence is interpolated linearly between grid points.

This approach requires no new GBT training beyond the original notebook's GBT-B training, and isolates the effect of monotonic post-processing relative to the **same calibrated GBT-B scores** on the extension evaluation cohort. PAV is a balanced correction, unlike a cumulative minimum that can only decrease probabilities. However, the correction values depend on the chosen grid and equal weights; interpolation enforces monotonicity between grid points but cannot establish that those probabilities are optimally calibrated. The correction grid is part of the inference method, not merely a plotting grid. A new station-time query requires scoring its full grid before answering an arbitrary threshold within the domain, increasing inference work.

The frozen grid includes 0 and 20 mm, all original six training thresholds, all five unseen evaluation thresholds, and extra intermediate thresholds, with denser coverage at lower rainfall. This reflects the strong concentration of rainfall near zero. The precise values are `MONO_GRID` in the notebook. This choice is **not grid-independent**; the report should explicitly acknowledge this limitation. The projection is equal-weighted over grid points, not weighted by interval widths or threshold prevalence.

### Method B: Multinomial rainfall-bin model + empirical within-bin CDF

Instead of learning independent exceedance probabilities, this model learns **one coherent distribution over mutually exclusive rainfall categories**. The MLlib multinomial logistic regression outputs nine nonnegative class probabilities that sum to one. Exceedance probabilities are computed by adding probability masses above the requested threshold. Consequently, as the threshold rises, the event set can only shrink and the predicted exceedance probability cannot increase. The monotonicity follows from a valid categorical distribution rather than a repair to separate binary predictions.

The nine categories are `{0}`, `(0,0.2]`, `(0.2,0.5]`, `(0.5,1]`, `(1,2]`, `(2,5]`, `(5,10]`, `(10,20]`, `(20,infinity)` mm. Zero is a separate class because around 94% of training station-times have no future 30-minute rain. The positive bins widen toward heavier rainfall because the observed target distribution is highly right-skewed. These boundaries largely retain the original training thresholds, allowing clear comparisons at the original decision thresholds. The upper tail remains a class because its mass contributes to all exceedance probabilities below 20 mm.

For thresholds **inside** a finite bin, a categorical model alone is insufficient: it predicts the total bin mass but not its internal allocation. The notebook estimates a **global empirical discrete conditional CDF within each bin** from *all valid 2017–2022 training targets*, not just the 2% sample used to fit the classifier. It aggregates `future_units` frequencies, builds cumulative fractions, and combines them with the predicted bin masses. This is a deliberate choice over uniform-within-bin interpolation: local diagnostics found a dominant 0.2-mm measurement pattern but also non-multiples, so the observed discrete distribution should be respected rather than spreading mass uniformly across arbitrary continuous values. The empirical CDF is common to all stations and times within a bin; it cannot capture conditional changes in within-bin rainfall shape, which is a substantive modelling limitation.

The classifier is MLlib `LogisticRegression(family="multinomial")`, using the same variant-B historical and calendar features as the GBT but **excluding threshold** (because threshold is applied to the predicted rainfall distribution after inference). It uses `StringIndexer`, `OneHotEncoder`, median `Imputer`, `VectorAssembler`, `StandardScaler`, and L2-regularized multinomial logistic regression. It trains on the same deterministic 2% **station-time** sample as the original final-fit models, without multiplying examples across thresholds. Its fixed `regParam=0.05`, `maxIter=60`, and `elasticNetParam=0` are implementation choices, not values optimized on 2024. No class reweighting is applied; that avoids intentionally distorting probability estimates for rare categories, although heavy-rain class estimation may still be unstable.

The empirical-CDF method supports `0 <= x <= 20` mm. For thresholds outside that interval, the implementation does not provide a supported prediction; it does **not** assume a distribution within the open-ended `>20` bin. At bin boundaries the rule is strict exceedance (`Y > x`, not `Y >= x`), using integer-scaled rainfall units to avoid ambiguous floating-point equality. The within-bin empirical CDF is a step function, not a smooth interpolation; this is intentional because the recorded target is discrete/quantized in practice.

## 3. Why the bin boundaries and empirical CDF are data-informed

A separate local, out-of-core DuckDB diagnostic was performed on the annual 2017–2024 CSVs, approximating the notebook cleaning and requiring six exact future readings for a valid target. Its training-period findings were roughly 34.54 million valid 30-minute targets, ~94% zero accumulation, median positive rainfall ~0.6 mm, and positive 99th percentile ~24.7 mm. Around 81% of positive training totals were exact multiples of 0.2 mm, but many values were not. The diagnostic indicated nontrivial counts in each of the proposed nine classes, even though the tail becomes comparatively sparse after 2% training sampling.

These diagnostics justify separating zero, concentrating resolution near light rain, retaining wider heavy-rain bins, and using an empirical rather than uniform within-bin distribution. **Do not cite these exact diagnostic figures as Spark-confirmed project results** without checking that they match Notebook 01/02 output; the diagnostics were run with a separate local implementation. The notebook itself constructs the CDF from Spark `train_base` when executed.

The existing 2023/2024 split is preserved. No future or 2024 information is used to choose the bin boundaries or estimate within-bin CDFs. The original unseen thresholds remain separate from the category boundaries and can test within-bin probability quality.

## 4. Fairness and evaluation protocol

The appended comparison evaluates three outputs on exactly the same deterministic **0.2% sample of valid 2024 station-times**:

1. `gbt_B_calibrated`: the globally score-calibrated original GBT-B predictions, before threshold-wise correction;
2. `gbt_B_threshold_isotonic`: those same scores, corrected by PAV across the fixed threshold grid;
3. `multinomial_B_empirical_cdf`: the nine-class MLlib model combined with training-derived within-bin CDFs.

The evaluation thresholds are `0`, `20`, the six original training thresholds, and five previously unseen thresholds. Every method is compared against identical strict-exceedance labels. The extension computes per-threshold PR-AUC, Brier score, F1 at a reference probability cutoff, weighted ECE, and adjacent-threshold monotonicity diagnostics; it checks that probabilities are finite/in `[0,1]` and that the two monotonic methods have no sampled-grid violations beyond tolerance. The full original 2024 holdout evaluation remains separately in the preserved Notebook 03 workflow; **do not present the extension's 0.2% comparison as a full-holdout evaluation**.

The methods have different model capacity and training targets. Thus a difference between corrected GBT and multinomial regression cannot be attributed solely to the presence or absence of a monotonicity constraint. Within GBT, comparing corrected versus uncorrected scores is the cleaner assessment of the correction's impact. The multinomial method is an alternative probabilistic formulation, not a controlled one-variable modification to GBT.

The code does not tune the new methods on the 2024 holdout, but its appended extension also does **not** run an independent 2023 selection campaign for the new approaches. Describe the new hyperparameters/grid/binning as fixed before test evaluation, and report all comparisons rather than claiming a newly optimized winner. One illustrative curve is selected deterministically by station and time, not cherry-picked for a large violation.

## 5. Statistical and computational considerations

- **Probability calibration:** Global score calibration is fitted for GBT-B from 2023 H2. The multinomial model has no additional fitted calibration stage in the extension. Its category probabilities may be imperfectly calibrated, especially for rare bins. The report should compare Brier and ECE rather than assume multinomial probabilities are calibrated simply because they sum to one.
- **Class imbalance:** Most examples are dry. The unweighted multinomial objective preserves the natural training class proportions but may perform poorly for rare heavy-rain categories. The fixed L2 regularization helps control variance; it does not guarantee statistically stable tail estimates.
- **Grid sensitivity:** Threshold-isotonic probabilities depend on the correction grid, which uses unequal spacing but equal PAV weights. Results should not be claimed to be invariant to different grid choices.
- **Inference cost:** GBT isotonic correction requires one GBT evaluation per correction-grid threshold for each *new* station-time feature vector, plus inexpensive PAV/interpolation. Once the corrected curve is computed, repeated threshold queries at the same station-time are cheap. The multinomial model generates one class-probability vector per station-time and answers thresholds via the empirical CDF lookup.
- **Scalability:** All primary fitting and scoring use Spark DataFrames and native MLlib estimators. The empirical CDF is formed by distributed aggregation followed by collection of **distinct rainfall amounts and counts**, not collection of all raw training rows. The distinct-value lookup must still fit driver/executor memory; its size is printed. Python UDFs for correction and lookup may add overhead compared with Spark-native expressions.
- **Domain:** Both extension methods define supported thresholds from 0 to 20 mm. The empirical method does not extrapolate beyond 20 mm; the GBT correction likewise rejects interpolation outside its grid.
- **Temporal validity:** Training uses 2017–2022, selection January–June 2023, calibration July–December 2023, test 2024. All features and labels are inherited from Notebook 02's validated future six-reading construction and time-boundary checks.

## 6. Operational notes and report-writing guardrails

Run order on a fresh environment is **00 → 01 → 02 → 03.1**. The new notebook contains the entire original Notebook 03 workflow and its extra training and scoring; it is **not** a lightweight extension that loads a previously saved GBT model. It therefore may have substantial computational cost, and successful execution has not been established in this document. A fresh `RUN_ID` should be used to avoid mixing with the previous completed run. Cloud resources, GCS paths, and package versions must follow the repository's environment documentation.

Notebook 03.1 saves the additional multinomial model, global GBT-B calibrator, extension scores, reliability summaries, JSON evaluation summary, and a representative probability-curve figure under `<ROOT>/monotonic_extension/`. Exact paths, constants, and source code remain authoritative in the notebook.

**What can be asserted before execution:** Both methods are constructed to be monotonic on the declared threshold domain, provided their model outputs are valid probabilities and their correction/lookup implementations behave as specified. The empirical-CDF method is a coherent categorical survival function. The PAV+linear interpolation method is non-increasing by construction.

**What cannot be asserted before execution:** That the new model has higher Brier/PR-AUC, that its rare-tail probabilities are well calibrated, that the added notebook runs successfully on the Dataproc image, or that it is more statistically stable than GBT. Do not invent results, runtimes, improvements, or completed-run claims. Report any failed or skipped cells, and distinguish the 0.2% extension cohort from the full original test evaluation.

## 7. Suggested report framing

A concise method paragraph can introduce the requirement that exceedance probabilities must be non-increasing in the threshold, explain that ordinary threshold-as-feature GBT does not enforce it, then present the two approaches: (i) projection of calibrated GBT scores onto a non-increasing curve, and (ii) multinomial modelling of rainfall categories with a training-derived within-bin empirical CDF. Discuss the nine-bin design and discrete distribution evidence as modelling rationale. Results should compare the original calibrated GBT against both alternatives at common thresholds, with Brier/PR-AUC, calibration and monotonicity, followed by limitations concerning bin coarseness, global within-bin shape, grid dependence, tail sparsity, and computational cost.
