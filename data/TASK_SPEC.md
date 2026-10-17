> This specification and historical log were retained from TASK.md when it was moved to TODO on 12/10/2026.
> See [TASK.md](TASK.md) for current work and instructions. The requirements below to wait for the Reports/Cost table,
> keep the cost gate closed, and reserve $40/$10 are old policy. The user's later instruction to "continue without
> worrying about cost" superseded the requirement to wait for cost reconciliation.
> The restriction to use only the Education account and credit 3LU63BPGY77JFDQQ remains in effect.
> Cloud states in the log are historical snapshots, not live checks.

# DSA5208 Project 2 — Singapore Rainfall Analysis and Prediction

Plan reviewed on 11/10/2026. This is an execution plan, not evidence that the data has been validated or that a cluster/model has run. Each phase has outputs and acceptance criteria. Statistical/modeling choices must be recorded before the final test.

## Active protocol v2 — 16/10/2026

Protocol v2 supersedes the v1 Task 2 sampling, split, calibration, and prediction rules below. The v1 run and its 2024 metrics are historical evidence; they are not v2 results and must not be used to make v2 choices. Because v1 2024 metrics were already observed, describe the v2 period as a **held-out 2024 evaluation period**, not an untouched blind test. Do not report v2 results until their run artifacts exist.

| Period | Role in v2 |
|---|---|
| 2017–2020 | Hyperparameter training; deterministic 10% screening, then full-fit survivor comparison on 2021 |
| 2021 | Hyperparameter validation |
| 2022 | Feature/model selection on a deterministic 10% sample |
| 2017–2022 | Refit the frozen winner on every valid row (100%) |
| 2023 | Fit the predetermined isotonic probability calibrator |
| 2024 | Final held-out evaluation after freeze |

Notebook 03 may read only the `search_train`, `hp_validation`, and `model_selection` partitions. It selects model family/features on 2022 and writes `artifacts/frozen_spec.json` with the chosen recipe and config/input/source hashes. Notebook 04 must verify those hashes before fitting the selected model on full 2017–2022. It always fits isotonic calibration on 2023, projects the six calibrated anchor probabilities onto the equal-weight non-increasing cone with PAVA, and linearly interpolates only within the declared threshold domain. It rejects out-of-domain thresholds. Notebook 04 opens 2024 only after the frozen model and calibrator exist.

No training-fraction scaling experiment is run. Report raw, calibrated, and constrained monotonicity, with strict violation rate and maximum increase for each. The constrained predictor must have zero strict violations by construction.

## 1. Official requirements and implementation choices

Assignment source: `data/project_description.txt`. Data source: [Historical Rainfall across Singapore, NEA](https://data.gov.sg/collections/2279/view).

| Topic | Official requirement |
|---|---|
| Data | Years 2017–2024; rainfall observations every 5 minutes at Singapore stations |
| Task 1 | Analyze rainfall patterns within a year and changes across years, with figures and discussion |
| Task 2 | Probability that a station accumulates **more than** the requested rainfall threshold in the next 30 minutes |
| Technology | Structured data processing with Spark; build and evaluate models with Spark MLlib |
| Report | Cleaning, patterns, features, model/results, computing environment/cluster configuration, limitations, and AI usage |
| Deliverables | Source + README; trained model; PDF listing every member's name and student ID |
| Group/deadline | Up to 3 people; 18/10/2026, 23:59 according to the Canvas description; confirm timezone/upload limit in Canvas |

**Individual implementation (1 person):** Mac + gcloud → GCS → Dataproc + Jupyter + Spark/MLlib → GCS outputs → source/model/PDF. Do not create a local cluster. The assignment allows local/GCP/Databricks; Google Cloud is the implementation choice, not an assignment requirement.

Old citation placeholders (`Lec6`, `Project-2-Singapore-Rainfall-An…`) have been removed. If citing lecture material in the report, check the source and record the exact slide title/page number.

## 2. Review findings

| Issue in the old draft | Clarification/fix |
|---|---|
| Upload all of `data/raw/`, with an unverified layout assumption | Keep data outside the repo; use the manifest to select exactly eight files from 2017–2024 and exclude 2016 |
| Assume timestamps are aligned to a 5-minute grid | Verify interval semantics and alignment before creating labels |
| Treat `observation_time ≤ t` as sufficient to prevent leakage | Check availability/revisions too; state real-time limitations |
| A year split may allow labels to cross into the next year | Purge horizons that cross a split/fold and check label endpoints |
| No policy for duplicates/gaps/coverage | Define the data contract, quarantine policy, denominator, and attrition |
| Treat heatmaps as proof that there is no bias | Use a stable station cohort, coverage checks, and sensitivity analysis |
| Use training thresholds in place of arbitrary input thresholds | Separate grid/domain/inference and test unseen thresholds |
| Tuning and calibration use insufficiently separated data | Use temporal selection and a separate 2023 calibration holdout |
| PR-AUC alone is insufficient for probability prediction | Add Brier/log loss/calibration, prevalence, and uncertainty |
| Cloud setup lacks IAM/network/runtime/cost guardrails | Add preflight, worker identity, pinned image, max-age, and artifact persistence |
| Saving a model does not prove it can be reproduced | Save the full pipeline/config; reload and infer from the submission package |

**Gates still requiring evidence:** whether timestamps mark the start or end of an interval; the meaning of update fields; timestamp-alignment rules; billing/project/IAM; compatible image/runtime; and the Project 1 LaTeX source. The academic-writing skill at `data/academic-writing-SKILL.md` was available and read while preparing the report outline. Do not assume these gates have passed.

## 3. Phase 0 — Repository and provenance

Local paths from `data/pwd`:

- Dataset root `/Users/macbook/Data/sg historical rainfall`, with CSVs in `raw/`.
- Course `/Users/macbook/Desktop/NUS/DSA5208`, with Project 1 in `project1/`.

Full local scan on 11/10/2026: `raw/` contains exactly the eight required CSVs for 2017–2024; no 2016 file was found at this path. An older inventory note listed nine files including 2016 and no longer matches the current inventory. NEA collection metadata lists nine annual datasets from 2016 to 2024; the assignment requires only 2017–2024 ([collection](https://api-production.data.gov.sg/v2/public/api/collections/2279/metadata)).

`scripts/build_manifest.py` read 8,047,044,701 bytes / 48,538,713 data rows, calculated SHA-256 for each file, and confirmed that all files share a 12-column header, with no incorrect field counts or missing/invalid timestamps. Per-file results, dataset IDs, and timestamp min/max are in `manifests/input_manifest.json`. All scanned record timestamps use `+08:00`. The observation ranges begin at `08:04:59` in 2017 and `00:00:00` in 2018–2024, and end at `23:55:00` each year. This was only an inventory/schema/timestamp-parsing scan; it **did not** check value ranges, duplicates/conflicts, station coverage, gaps, per-station timestamp alignment, or interval semantics. Do not use min/max as evidence that a dataset is complete.

The initial header/sample showed update delays (2017 observation `08:04:59`, update `08:13:31`; 2024 observation `00:00:00`, update `00:05:26`). This is a reason to continue auditing availability/alignment; it does not prove that `timestamp` is the interval end.

Keep raw data immutable and outside the repo. Select only the eight files `HistoricalRainfallacrossSingapore{2017..2024}.csv`. Do not silently use 2016 as warm-up data. Code reads `DATA_ROOT` from config; do not hard-code personal paths. The manifest must include dataset ID/source URL, year, filename, bytes, SHA-256, retrieval time, schema, and actual time range. Compare the local/GCS manifests; an object listing alone does not prove that the upload is correct.

```text
sg-rainfall-distributed-analysis-and-prediction/
├── README.md
├── requirements-local.txt
├── requirements-cluster.txt
├── config/
│   ├── cloud.env.example
│   └── experiment.json
├── data/TASK.md
├── notebooks/
│   ├── 00_environment.ipynb
│   ├── 01_data_quality.ipynb
│   ├── 02_task1_weather_patterns.ipynb
│   ├── 03_task2_features.ipynb
│   ├── 04_task2_models.ipynb
│   └── 05_final_evaluation.ipynb
├── src/rainfall/             # config, cleaning, features, models, metrics, predict, plotting
├── scripts/
├── tests/
├── figures/
├── results/
├── models/
└── report/                  # main.tex, references.bib
```

**Jupyter Notebooks are the only entry point for running project code and viewing outputs.** Reusable functions live in `scripts/` or `src/` and are imported/called from notebooks; do not run analysis from a terminal, `spark-submit`, or batch script. Required Cloud SDK commands are also invoked from Python notebook cells using `subprocess`, with status/results printed to cell output. Save notebooks with the necessary outputs; do not write credentials or sensitive content into notebooks. Each run has a `RUN_ID`, Git commit, input manifest hash, and config hash; write output under a distinct run prefix and do not overwrite final results. Do not commit `skills/`, `.codex/`, `.vscode/`, `AGENTS.md`, `config/cloud.env`, credentials, or large raw/cache files. Sample plans/configs and small result tables may be committed.

**Acceptance:** manifest covers all eight years; paths are portable; entry point and execution order are clear; repo contains no credentials/raw data.

## 4. Phase 1 — Cloud preflight and budget

### 4.1. The only credit and billing account authorized for use

**User constraint: use only the education credit below and the billing account that contains it. Do not use any other billing account or credit, or spend money outside this credit.** This constraint applies to the entire project: compute, Dataproc, GCS, disks, networking, logging, and supporting resources.

Credit ID `3LU63BPGY77JFDQQ` is the only ID authorized by the user, who specified the Billing Account named Education. The table retains the snapshot from before the cloud run on 11/10/2026 and separately records the latest Credits snapshot seen on 12/10/2026. The newer balance alone was not sufficient to approve a job because Reports, the Cost table, and pending usage had not been reconciled:

| Attribute | Provided value |
|---|---|
| Credit name | Scalable distributed computing for data science aug26 |
| Credit ID | `3LU63BPGY77JFDQQ` |
| Status / remaining at pre-run snapshot | Available / 100% (historical only) |
| Remaining / original value at pre-run snapshot | $50.00 / $50.00 (historical only) |
| Latest Credits page shown 12/10/2026 | Available / 96%; $47.91 remaining / $50.00 original |
| Type | One-time |
| Usage scope | Any service on this billing account |
| Start / end date | 17/08/2026 / 14/08/2027 |
| Credit application type | Net pricing |

The Credit ID is not the Billing Account ID. Read-only gcloud inventory on 11/10/2026 identified the account specified by the user as **Billing Account for Education**, ID `01DAD8-AE5F1A-E3910A`, status open, currency USD. Do not use another account. Gcloud confirms the account name/ID/status; it does not select a Credit ID for each job. The Console snapshot from before the 11/10/2026 run showed credit `3LU63BPGY77JFDQQ` Available, 100%, with USD 50.00 remaining; that tab had been open before the run, so the figure is historical. The Credits page open in Chrome on 12/10/2026 showed the correct Education account and 96%, USD 47.91 remaining of USD 50.00. The USD 2.09 difference is a decrease at the account-level credit scope; do not attribute it to this project without a cost breakdown. Reports, the Cost table, and pending usage were not verified in the latest check.

Google automatically applies promotional credits to eligible costs; gcloud cannot select a Credit ID per job. Check Credits and cost details for the correct account to verify which credit was applied ([education credits](https://docs.cloud.google.com/billing/docs/how-to/edu-grants), [cost details](https://docs.cloud.google.com/billing/docs/how-to/cost-table)). If multiple promotional credits share the same scope and it has not been verified that only the specified credit will pay, stop before the next billable step and clarify. Do not redeem another credit, switch accounts, add a payment method, or switch to paid billing to continue.

The runnable project is `sg-rainfall-dsa5208-261011`; on 11/10/2026, notebook preflight verified `billingEnabled=true` and the correct billing account `01DAD8-AE5F1A-E3910A` before creating resources. The Credits page currently shows USD 47.91 remaining, but Reports, the Cost table, and pending usage have not been reconciled; therefore, the USD 2.09 decrease, project gross usage, account-level pending charges, and current storage cost are unknown. The billing gate remains closed and no additional billable resources should be created. Credits are applied automatically at billing-account scope; a Credit ID cannot be assigned to an individual job. Before the next cloud run, inspect cost details for the correct account and confirm that the USD 40 spending limit and USD 10 reserve remain intact after usage/pending charges are counted. Do not change billing accounts, redeem another credit, add a payment method, or spend money outside the specified credit. Remove the cluster and any billable resources after exporting results; final cost reconciliation may be delayed.

### 4.2. Configuration and preflight

Project/account/region/run parameters are set in `notebooks/00_environment.ipynb`; do not use a credential file or `config/cloud.env` as a separate execution path. Preflight verified the correct Education billing link before creating the bucket/cluster; the project, bucket, VM service account, region, and quota are recorded in outputs. The dedicated VM service account initially lacked permission to read billing status, so a project-scoped custom role containing only `resourcemanager.projects.get` was added, then the exact billing account was verified again. The private Dataproc/YARN cluster from the initial run was deleted according to the latest read-only inventory; there is no current cluster. Notebook 01 compared the actual byte-level SHA-256 hashes of all eight CSVs with the manifest. Usage and credit reconciliation after cleanup is still required.

Notebook preflight: check regional CPU/disk quotas, subnet/Private Google Access/firewall for internal traffic, cluster-creation permission, and `iam.serviceAccounts.actAs`. The VM service account needs `roles/dataproc.worker` and appropriate read/write access to data and staging/temp buckets; do not put JSON keys in the notebook. Mark a gate as passed only when inspection output exists. Source: [Dataproc service accounts](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts).

### 4.3. Operating limits within the credit

The planned spending target is at most **$40**, with at least **$10** reserved for pending usage, storage, export/download, and cleanup, in the same currency as the credit. This is a conservative operating threshold, not a spending cap guaranteed by Google. When remaining credit is ≤ $10 or estimated session + pending/retention costs exceed the allowed amount, do not start another job/session; persist/download outputs and stop the project's billable resources.

Create a project-scoped alerts-only budget on the correct account, and track usage costs separately from credit application; do not let a net cost of zero after credits hide the amount of credit spent. **An alerts-only budget does not block spending** ([Billing budgets](https://docs.cloud.google.com/billing/docs/how-to/budgets)). Estimate VM, Dataproc fees, disks, GCS operations/storage/network, and supporting resources using regional pricing on the run date ([pricing](https://cloud.google.com/dataproc/pricing)). Record credit snapshots at the start/end of each session and usage not yet reflected in Console; billing may be delayed ([cost details](https://docs.cloud.google.com/billing/docs/how-to/cost-table)).

The cluster must have both an idle timeout **and** a maximum lifetime, and must still be deleted manually when finished. Deleting the cluster does not stop GCS/supporting-resource charges. Do not create a cluster until preflight and cost estimates pass.

**Acceptance:** `BILLING_ACCOUNT_ID` is locked to credit `3LU63BPGY77JFDQQ`; no other account/credit is used; linkage and balance/scope are reconciled; the budget covers pending/storage/cleanup; quota/IAM/network are correct. If not met, perform no billable operations.

## 5. Phase 2 — GCS, Dataproc, and Jupyter

The bucket is private, uses uniform access, and is in the same region as compute. Create the bucket and upload the exact manifest and eight CSVs (2017–2024) using separate cells in `00_environment.ipynb`; each cell prints status and the object list to its output. Do not upload 2016 or data outside the manifest.

Layout: `raw/`, `manifests/`, `processed/<RUN_ID>/`, `features/<RUN_ID>/`, `notebooks/`, `models/<RUN_ID>/`, `results/<RUN_ID>/`, `staging/`, `temp/`.

Cloud runs read paths from the manifest rather than using a wildcard that could select the wrong year/object. GCS raw data + manifest are the input source; do not copy raw data to every worker.

Initial cluster candidate: 1 `e2-standard-4` master + 2 workers of the same type (4 vCPU, 16 GB RAM/VM), 100 GB disk/VM, no GPU. This configuration needs a price check and smoke run; the older 8 GB master leaves little headroom for Jupyter/driver. Do not increase compute before checking the Spark plan/skew/cache/partitions.

Choose a supported image and check Jupyter/Python/Spark; pin the **exact image version** after the smoke run. When testing an alias, record the resolved version. Use local `python3` 3.14 for compatible utilities; use the Python version bundled with the Spark-compatible cloud image. Do not force Dataproc to 3.14 or install a newer PySpark over its runtime ([image versions](https://docs.cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters)).

Create the Dataproc cluster in a dedicated cell in `00_environment.ipynb` after the price check and cost guard pass. Enable the Jupyter component and private networking, with one master and two workers using the candidate configuration; print the cluster name, region, image version, VM/disk, and lifecycle settings in notebook output for review. Do not create a cluster by default if the cost estimate or credit gate has not passed.

`3h` is the initial session lifetime. Idle calculation is based on Dataproc/YARN activity; a notebook application may keep the cluster active. Max-age can still delete the cluster while a job is running, causing the job to fail. Save checkpoints/outputs by phase and monitor lifecycle ([scheduled deletion](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion)).

An internal-IP cluster requires Private Google Access and correctly configured internal networking. Public PyPI/Maven downloads may fail without internet egress; prefer preinstalled dependencies or compatible wheels on GCS, and account for additional cost if using NAT ([network configuration](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/network)). Pin and import-check SciencePlots/Matplotlib against cloud Python before a full run.

Open Console → cluster → Web Interfaces → Jupyter/JupyterLab through Component Gateway. Save the notebook on GCS and export it to the repo after the session; do not keep artifacts only on the cluster's local disk ([Jupyter component](https://docs.cloud.google.com/dataproc/docs/concepts/components/jupyter)).

In the Dataproc notebook, record image, Spark/Python/Java/OS, region, machine types/disks, service account, application ID/master, actual executors, and Spark config. Set `spark.sql.session.timeZone=Asia/Singapore`. The notebook smoke run reads one CSV with an explicit schema, creates/reads a small Parquet dataset, and checks MLlib/imports/GCS. Spark master must be YARN, with executors running on workers. Display all stdout, tables, and figures needed for analysis in the notebook; save the executed notebook to GCS and export it to the repo after the session.

**Acceptance:** worker identity can read/write GCS; Spark runs distributed; notebook persists; environment/lifecycle are recorded.

**Spark Web UI evidence for every run:** track each notebook action that creates a
Spark application/job until it reaches a terminal state (succeeded, failed, or
canceled). After the cell/action finishes, open the correct application by
Application ID: use the live Spark Web UI if the notebook/application is still open,
or the **Spark History Server** through Web Interfaces/Component Gateway if the application
has finished. Inspect the Jobs tab. Capture a screenshot of the Jobs overview and a detail
page for every Spark Job ID; screenshots must show Application ID/Job ID, status,
duration, and available
stage/task information. If a job is still running after the command returns, wait for a terminal state
and capture it then. Do not use the Dataproc Jobs page or
YARN in place of Spark UI screenshots. Enable/check Spark event logging and the History Server
before batch runs so completed applications remain viewable. Save screenshots to
`report/evidence/spark-ui/<RUN_ID>/` with filenames containing Application ID, Job ID, and
status; record each notebook action and screenshot path in
`report/evidence/spark-ui/index.csv` (one row per notebook action; repeat the
Application ID if the same app has multiple actions, and record every Job ID and detail screenshot created by
that action). Capture failures/cancellations too, and record their
actual state;
do not present them as successes. Local cells that do not create a Spark application
are outside this requirement. See [Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)
and [Dataproc Component Gateway](https://docs.cloud.google.com/managed-spark/docs/concepts/accessing/gateways).

## 6. Phase 3 — Data contract and cleaning

NEA states that timestamps are SGT, records may be missing, and the data has not undergone climate quality control; do not treat the raw data as climate-quality data ([NEA collection](https://data.gov.sg/collections/2279/view)). Confirm each file's schema; sample header:

```text
date, timestamp, update_timestamp, station_id, station_name, station_device_id,
location_longitude, location_latitude, reading_update_timestamp,
reading_value, reading_type, reading_unit
```

Spark reads CSVs with an explicit schema and retains malformed rows for auditing; do not repeatedly use `inferSchema` across eight years. Preserve the original/source_file and the reason for quarantined rows.

| Field | Contract |
|---|---|
| Station | ID is not null/blank; audit changes to name/device/location |
| Observation time | Parse timezone offset; retain original; derive local year/date and compare with `date` |
| Interval end | Create only after verifying timestamp/interval semantics and alignment |
| Availability | Use update fields as a proxy after verifying their meaning; flag missing/invalid/update-before-observation |
| Rainfall | Finite numeric value, nonnegative, unit `mm`, correct 5-minute total reading type |
| Coordinates | Finite, longitude/latitude in the correct order; audit anomalies/relocations |
| Key | `(station_id, interval_end)` is unique after the duplicate policy |

The `08:04:59` timestamp shows that records must not automatically be floored into 5-minute buckets. The column legends on the [2017 page](https://data.gov.sg/datasets/d_1990a5a1aeaf3dd243cf4dae294a61c4/view) and [2024 page](https://data.gov.sg/datasets/d_a0b69d3e02576a1fd0ab673e71f83507/view) do not currently explain whether timestamps mark interval start/end or what the update fields mean. Audit seconds/spacing by year/station and look for source documentation. Base alignment tolerance on evidence, freeze it before model selection, and retain the raw timestamp/offset. Do not combine readings from different intervals just because they fall in the same bucket. If semantics remain unclear, the gate has not passed and official labels must not be created.

Retain one copy of exact duplicates. Quarantine conflicting readings for the same key or resolve them using an evidence-backed version policy; do not randomly use `dropDuplicates` or sum duplicates. Task 1 may use a canonical final reading; Task 2 history needs the version available at prediction time. A final-only export cannot reconstruct the full publication history.

Missing/gaps are not zero. An extremely large but finite/nonnegative value is not sufficient reason to exclude it; rainfall extremes are part of what the model should learn. Flag/exclude values only using sourced quality rules and sensitivity analysis; do not winsorize labels to make the task easier.

Compute completeness on the aligned grid: report active station-periods and a fixed full-calendar denominator; first/last observations do not confirm station commissioning/retirement dates. A complete day has 288 slots; account for leap years. Quality summary includes row counts before/after, invalid/conflicting/duplicate records, gap lengths, station×month/year coverage, and metadata drift. Retain excluded counts/reasons.

Write validated Parquet partitioned by year, adding month only if profiling supports it; avoid station+timestamp partitions that create many small files. The snapshot includes the manifest/config/version/quality summary. Later phases read Parquet; reread CSV only when the cleaning rule changes.

**Acceptance:** all eight years; schema/unit/time/key checks pass; rejected rows have reasons; coverage has a denominator; interval semantics have evidence.

**Implementation started:** `scripts/run_quality_audit.py` provides a function for notebooks to import; it reads the exact paths in the manifest, uses an explicit CSV schema, and keeps raw data immutable. The audit is expected to output `quality_by_year`, `csv_parser_corrupt_candidates`, `reading_type`/`reading_unit` distributions, duplicate-key groups (with a conflict flag, min/max, and count of distinct readings), station-level metadata variations, spacing deviations from 300 seconds, and `audit_metadata`. Duplicate/timestamp and spacing results are currently investigation candidates only; the job does not automatically deduplicate, quarantine, or assign timestamps as interval ends. `update_timestamp` and `reading_update_timestamp` are checked only for parseability/order and are not treated as verified publication times. Output is blocked if placed under `raw/`.

Open and run `notebooks/01_data_quality.ipynb` in Dataproc Jupyter after preflight passes. The notebook loads the manifest, verifies the exact eight GCS objects using SHA-256, creates a SparkSession on YARN, calls `run_audit(...)` in a cell, and displays the application ID, Spark Job ID(s), final status, output path, and Spark Web UI link. Do not run the module with `spark-submit` or from a terminal. Use a new run prefix for each attempt.

**Execution status on 11/10/2026:** notebook 00 created project `sg-rainfall-dsa5208-261011`, verified its link to the correct Education account, and created a private bucket and private Dataproc cluster. Notebook 01 attempt 2 ran Spark 3.5.3/YARN; its cell verified that actual byte-level SHA-256 hashes matched the manifest for all eight CSVs. The audit reported 48,538,713 rows; all `reading_type`/`reading_unit` values were `TB1 Rainfall 5 Minute Total F`/`mm`, and the manifest scan found 0 rows with an incorrect field count. Action `raw_quality_audit` completed in 858.697 seconds in application `application_1791706788393_0001`, with Job IDs 0–20 SUCCEEDED. The subsequent `deterministic_cleaning` action failed due to a YARN ApplicationMaster timeout and Spark listener OOM. However, Parquet `cleaned/` had already written `_SUCCESS` and all 2017–2024 partitions before the failure. Outputs `rejected/`, `quality_by_year/`, `exclusion_counts/`, `alignment_distribution/`, and `coverage_and_gaps` were not created; therefore, retained/excluded reconciliation and eight-year coverage checks in Notebook 01 remain incomplete. Do not mark Phase 3 complete based only on the Parquet marker. Spark UI screenshots were not captured; the Mac was locked during review, and API metadata is not a substitute for real screenshots. The corrupt-record flag covers parser errors only; a separate manifest scan is needed to detect too few/many tokens.

## 7. Phase 4 — Task 1: Weather patterns

For each analysis, record its purpose, cohort/denominator, statistic, observation, interpretation, and limitation. Do not create a separate set of research questions; follow Task 1 directly. Do not sum all stations and call it “total Singapore rainfall.”

| Analysis | Output/method |
|---|---|
| Completeness | Station×year/month heatmap, gap distribution, exclusions |
| Seasonality | Station-month totals with coverage; wet probability = valid wet / all valid; intensity conditional on wet |
| Daily cycle | Month×SGT-hour heatmap with denominators and station weighting |
| Across years | Annual station summaries on a shared cohort/comparable coverage |
| Spatial | Map wet frequency/intensity/extreme rate using audited coordinates, with coverage |

The primary comparison uses equal station weighting within a common cohort. Declare the coverage cutoff (95% is a candidate) and run sensitivity checks with other cutoffs/cohorts; retain a full-cohort descriptive view. A heatmap does not prove that missingness introduces no bias; complete-window filtering may exclude storm periods.

An observed total in a month with missing data is not a full-month total. Coverage-adjusted estimates require assumptions about missingness and comparison with observed totals. Take the extreme cutoff from training data or a sourced external definition; a training quantile does not become an official warning threshold by itself.

Task 1 describes all of 2017–2024. Freeze the Task 2 design before descriptive exploration of 2024 to avoid using test-year findings to choose features/thresholds/models. Eight years support observed variation only, not strong climate-trend/causality claims. Export figures + aggregate tables + plotting config; collect only small aggregates.

**Acceptance:** within-year/across-year findings; cohort/denominator/unit/caption/method/limitations are clear; bias sensitivity has evidence.

### 7.1. Detailed EDA and required artifacts

EDA directly supports Task 1; do not add research questions. Prioritize coverage, distribution, monthly/hourly, and annual comparison in the main report. Tail, complete-day, station-coordinate, and wet-spell panels provide sensitivity/supplementary evidence; include them in the appendix when useful, rather than lengthening the main text with repetitive plots.

| Content | Calculation and support | Artifact |
|---|---|---|
| Distribution | By year: valid/dry/wet counts, wet fraction, mean valid rainfall, conditional wet mean, wet median/P90/P95/P99, max. State pooled-observation weighting. | `distribution_by_year`, `rainfall_histogram`; `rainfall_distribution.pdf` |
| Tail | Fixed bins for wet readings; log y to inspect the tail. Reference = wet P99 from **2017–2022**, held constant across years. Exceedance denominator = valid readings, strict `>`. Do not call training P99 an official warning threshold. | `rainfall_tail.pdf` |
| Coverage | Calendar with 288 slots/day, including leap days; periods with zero observations must be in the denominator. The reference network may include inactive stations; without operating dates, do not call low coverage downtime. | `coverage_summary`, `coverage_heatmap.pdf` |
| Daily sensitivity | Use only days with all 288 unique exact-grid readings. Average daily rainfall by station-month, then average stations equally; report complete/observed and complete/full-calendar station-days. | `daily_completeness`, `complete_daily_patterns.pdf` |
| Month | Equal eligible-station mean of observed total, wet frequency, and conditional wet intensity; primary station-month coverage ≥95%. Report station support and mean coverage alongside the measurements. | `monthly`, `monthly_frequency_intensity.pdf` |
| Hour | SGT **interval-end hour**; monthly **and hourly-group** coverage for each station-month ≥95%. Expected slots per hour = 12×days. Audit includes hours with no observations; do not replace missing rainfall with zero. Equal station-year-month-hour weighting in heatmaps; yearly curves use equal station-month weighting. | `hourly_coverage`, `hourly_by_year`, `monthly_hourly_patterns.pdf`, `hourly_by_year.pdf` |
| Annual comparison | Same station IDs in every year; fixed cutoffs 90/95/99%. Export the station count even for an empty cohort, observed total, and wet frequency. Keep the full observed changing-cohort view separate. | `annual_cohort_support`, `annual_common_cohort`, `annual_descriptive`, `annual_patterns.pdf` |
| Stations | Plot only stations with unchanged coordinates throughout the period; annual coverage ≥95%; equal average across eligible years. Record eligible years/valid slots; do not interpolate area or infer causality. | `station_spatial_summary`, `station_map`, `station_spatial_patterns.pdf` |
| Wet spells | Consecutive positive readings exactly 300 seconds apart at the same station. Duration = 5×records minutes; separate gaps and boundary-censored sequences. Count and duration median/P90/P99/max by start year. Do not treat gaps as dry or sequences as meteorological storms. | `wet_spell_summary`, `wet_spell_patterns.pdf` |

**Time convention accepted by the user:** timestamp marks the end of a 5-minute interval; this is a project assumption, not confirmation from NEA. Calendar month/day/hour in EDA are grouped by the interval-end timestamp. Totals are labeled by the timestamp's calendar period and are not presented as verified meteorological daily totals.

**Jupyter order and outputs:** `02_task1_weather_patterns.ipynb` runs EDA for 2017–2023; `05_final_evaluation.ipynb` runs EDA for all of 2017–2024 after Task 2 is frozen. Spark materializes aggregates once, then plots/exports from small tables; the driver collects at most 20,000 rows per table and never collects raw records. The notebook displays tables/figures/findings; JSON on GCS and CSV + PDF/PNG preserve full evidence even when `.show(20)` truncates display. Figures use SciencePlots and do not require TeX on the worker.

`findings.json` must include actual peaks/troughs, units, support, and common-cohort availability calculated from aggregates; do not fill it with illustrative findings before a run. After each action, review the **Spark Web UI / History Server Jobs summary and every Job detail**, then capture real screenshots showing application/job IDs, status, duration, and stages/tasks. Event metadata is only a cross-reference index, not a replacement for screenshots.

**EDA acceptance:** at least one within-year finding, one annual comparison with sensitivity (or an explanation for the lack of a common cohort), and one coverage/distribution finding, all with real measurements. Report peak–trough in mm or percentage points, comparison sample, support, and limitations; do not draw conclusions from plot colors alone. If months/hours are missing, list the months/hours represented; do not treat them as zero or claim a complete comparison.

**Execution status on 11/10/2026:** Notebook 02 attempt 1 stopped before the EDA action because SciencePlots was not in `sys.path`; application `application_1791706788393_0002` created no Spark job. A wheel was installed in the project vendor directory, and Notebook 02 attempt 2 reran on Parquet `cleaned/`: action `task1_pretest_eda` in application `application_1791706788393_0003` completed in 498.084 seconds, with all 241 Spark jobs SUCCEEDED. The notebook has outputs; CSV/JSON and PDF/PNG are saved in `report/tables/pretest/` and `report/figures/pretest/`. Results for 2017–2023: 84 station IDs in the fixed network; mean network coverage per year was 39.4%–80.8%; there was no common station cohort even at a 90% cutoff; the peak month differed by year, and 15:00 SGT had the highest wet frequency in 6 of 7 years. `report/main.tex` was updated with results, denominators, and interpretation limits. The full eight-year comparison must run after Task 2 is frozen. Spark History UI screenshots of the Jobs summary and 241 Job details were not saved to files because the Mac was locked; this acceptance item remains open, and the History API is not a substitute. Notebook 03 completed: app `application_1791706788393_0004`; the fixture action took 47.949 seconds, with 18 successful jobs and all 7 leakage/target fixtures passing; action `base_features_and_labels` took 754.368 seconds, with Job IDs 18–26 all SUCCEEDED. Parquet `base_features/` and `sample_attrition/` contain `_SUCCESS`. Eligible base station/time keys: train 32,798,917; selection 3,325,499; calibration 3,393,373; 2024 test 6,374,476. Invalid windows due to missing future readings in each split: 814,899; 11,350; 11,884; 31,408, respectively. These are window-completeness counts, not positive prevalence. The notebook materialized six future sums for the 2024 partition before freeze, but displayed only `label_valid` counts; it did not inspect/use 2024 prevalence, model scores, or metrics. This process limitation must remain transparent; do not call the test fully blind. Do not read the 2024 partition for scoring until the frozen manifest is recorded. App4's Jobs summary was opened directly in Spark History Server and showed 27 completed jobs; Job 20 detail showed SUCCEEDED, 9.6 minutes, and a completed stage with 29/29 tasks. Two screenshots were captured and displayed in the interaction but not exported to files under `report/evidence/`. The summary file and all 27 Job detail files are still pending.

## 8. Phase 5 — Target and inference contract

Once semantics are verified, `r(s,u)` is the rainfall total for interval `(u−5min,u]`; `t` is the decision time on the 5-minute SGT grid:

\[
R_{30}(s,t)=\sum_{k=1}^{6}r(s,t+5k\text{ min}),
\qquad Y(s,t,\tau)=\mathbf{1}[R_{30}(s,t)>\tau].
\]

Keep strict `>`: a value equal to the threshold is negative. Use numeric precision appropriate to the actual resolution to avoid equality errors. The label is future cumulative rainfall, not only the reading at `t+30`; it excludes the interval ending at `t`.

Valid labels require exactly six distinct, aligned/contiguous intervals with valid rainfall. Check timestamps, not just the count. `lead(1..6)`/`rowsBetween(1,6)` can select readings more than 30 minutes away when data has gaps. Use an explicit grid or epoch-second time range plus completeness checks. On the correct grid, the label range is `[300,1800]`, and the past-30-minute range is `[−1500,0]`; history also needs availability filtering. Range boundaries are inclusive ([Spark window API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rangeBetween.html)).

Define a clear prediction-grid policy, including when the current reading is missing; excluding these times reduces reported coverage. Do not fill/interpolate future rainfall. Report sample attrition by split/station/month due to future gaps, history, and boundary purging.

`predict.py` accepts `station_id`, `prediction_time`, and `threshold_mm`; it retrieves available history/metadata, builds the same features, and returns a probability. Return a clear status for unseen stations/missing history/invalid times/out-of-domain thresholds. Reject off-grid times; do not silently round them and change the horizon.

Candidate grid `0.2,0.5,1,2,5,10 mm` is only a training design; finalize the grid/domain from **training data** based on resolution/positive support, and save it in config. Threshold is a numeric feature. The API accepts numeric thresholds within the domain; also evaluate intermediate thresholds not used in training, but do not promise extrapolation outside the domain. Climatology uses the empirical training survival distribution to handle arbitrary thresholds.

Compute base features/label once per `(station,t)`, then expand across the grid. Copies for different thresholds stay in the same split/sampling group. Record base samples/expanded rows and label monotonicity; the copies are not independent.

**Acceptance:** correct horizon/strict inequality/completeness/domain; fixtures for dry/wet/gap/duplicate/equality/off-grid cases; inference returns a probability.

## 9. Phase 6 — Features and availability leakage

Feature readings must satisfy **both** `interval_end ≤ t` and `available_time ≤ t`. An older observation may not have been published at `t`; the local sample contains delays, so do not assume `rain_t` is available.

Verify the meaning of update fields before doing as-of lookup; do not use later revisions. If only the final revision is available, state the limitation and use a conservative proxy/delay sensitivity based on training data. If availability cannot be verified, describe the result as **retrospective event-time prediction** assuming immediate observation; do not claim real-time performance. Add availability-aware sensitivity when metadata permits. Freeze the mode/assumptions in results/model metadata.

| Category | Candidate features |
|---|---|
| Station | ID, time-valid coordinates, unseen handling |
| Temporal | Fractional-hour sin/cos; day-of-year sin/cos with the year's period; month |
| History | Aligned lags 5/10/15/30/60 min; totals 15/30/60/180 min; max/mean/wet count |
| Missingness | Valid count/coverage in window, missing indicators, latest-reading age |
| Query | Numeric `threshold_mm` |
| Optional spatial | Neighbour mean/max/wet count + valid-neighbour count, as-of filtered |

Compute lags by elapsed time, not row position. State rolling-window boundaries. Impute history using a rule fit on training data plus missing indicators; do not backfill from the future. Build windows with context across year/month boundaries; do not lose history by processing partitions separately. Only the label must lie entirely within the split.

Metadata/neighbour locations must be available at the corresponding time; do not select neighbours based on 2024 correlation. Fit indexers/scalers/imputers/feature selection and learned statistics only on the fold's training data.

Leakage fixtures: changing rainfall/update/metadata after `t` must not change features at `t`; an older observation published after `t` is excluded in availability-aware mode; labels use future readings; windows do not cross gaps/mix stations. Also check deterministic reruns and key uniqueness.

**Acceptance:** feature dictionary records source/window/availability/missing rules; fixtures demonstrate the time constraints.

## 10. Phase 7 — Temporal split, tuning, and calibration

| Subset | Prediction times | Role |
|---|---|---|
| `search_train` | 2017–2020 | Deterministic 10% hyperparameter screening; refit survivors on all valid rows |
| `hp_validation` | 2021 | Freeze LR/RF/GBT hyperparameters |
| `model_selection` | 2022 | Select the strongest feature/model candidate on a deterministic 10% sample |
| `calibration` | 2023 | Fit the predetermined isotonic calibrator on a deterministic 10% sample |
| `test` | 2024 | Final held-out evaluation only |

After model selection, refit the selected family/variant from scratch on all valid 2017–2022 rows. Hyperparameter screening uses a deterministic 10% sample before survivor refits; the 2022 model/feature comparison and 2023 calibration each use a deterministic 10% sample of their designated partitions. No training-fraction scaling study is run.

For split `[start,end)`, retain `t ≥ start` and `t+30min < end` because endpoint `end` belongs to the next split. Purge incomplete horizons at every calendar-year boundary; audit prediction/label endpoints. History before `start` is valid past information. To claim simulation at an actual training cutoff, training labels must also have been available before fitting; historical final revisions do not establish this by themselves.

Do not use random CrossValidator/TrainValidationSplit. Keep threshold copies/stations in the same time block in each fold. For hyperparameter screening, sample base station-time keys deterministically before threshold expansion; use the same seed across candidates. Keep up to two configurations per family within the predeclared PR-AUC tolerance, refit those configurations on all 2017–2020 search rows, then choose hyperparameters on 2021. Use the predeclared 10% deterministic sample for model/feature selection and calibration.

Use the configured LR/RF/GBT grids for hyperparameter screening, using feature variant A for LR and B for RF/GBT so each tuned recipe matches a compared candidate. After hyperparameters are frozen, compare climatology, LR-A, RF-B, GBT-A, and GBT-B on the deterministic 10% 2022 sample. Fit those candidates on the same deterministic 10% sample of 2017–2021 rows with the same family parameters. Record candidate count, sampled row counts, and measured fit time.

After 2022 candidate selection, freeze the winning candidate and its exact algorithm, feature variant, and parameters. Notebook 04 must use that frozen winner and fit it on 100% of valid 2017–2022 rows; fit no final-model transforms on 2023 or 2024.

Freeze before testing: input/config/source hashes, cleaning/alignment/availability, domain/grid, features, split cutoffs, sampling fractions and seed, selected pipeline/hyperparameters, calibration method, PAVA/interpolation rule, selection objective, metric settings, and Git commit. Prior v1 2024 metrics are not part of v2 model decisions; disclose that the calendar period was previously observed.

**Acceptance:** no horizon crossing; learned transforms fit on the correct training data; separate calibration holdout; frozen manifest before testing.

## 11. Phase 8 — Models and distributed execution

Primary candidates: climatology, Spark MLlib LogisticRegression, RandomForestClassifier, and GBTClassifier. Climatology is the empirical training survival distribution by station/month/hour, with smoothing/support policy and fallback to broader/global groups for sparse/unseen cases; fit the lookup on the designated training partition and save it.

LR: StringIndexer (`handleInvalid=keep`) → station OneHotEncoder → imputation/assembly → appropriate scaling → LogisticRegression. Keep sparse vectors; do not center one-hot features and cause densification. GBT does not need scaling; indexed categorical stations must retain metadata and use `maxBins ≥ categories`, accounting for the unseen bucket; one-hot is an alternative ([GBT API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.GBTClassifier.html)).

Save the **PipelineModel** with learned preprocessing. Unseen `keep` avoids encoding errors but does not prove generalization; report counts/fallback. Fix label mapping at 0=no exceedance, 1=exceedance; export `probability[1]`, not the predicted class as a probability.

Use natural prevalence by default. If oversampling/class weights are used, record the policy and its effect on probabilities; calibration/evaluation use natural validation/test prevalence. Do not rebalance the test set.

Use Spark SQL/DataFrame for features/windows/metrics and `spark.ml` for training. API links in this plan point to `latest`; when implementing, check the documentation for the Spark version pinned on the cluster. Do not collect/toPandas all raw/features to the driver. Collect only bounded aggregates/curves. Build base features before threshold expansion; persist reused data when beneficial and unpersist when finished; avoid repeated CSV scans/Python UDFs where built-in SQL operations exist.

Record rows/bytes/partitions, stage runtime, and executor evidence; inspect window/join plans and skew. Station-only windows may group multiple years into one large task. If chunking by time, add a 180-minute history halo/30-minute future halo, then trim to owned rows; do not compute windows on chunks missing context. Choose file/shuffle sizes by profiling; do not assume one setting is optimal.

**Acceptance:** climatology and ML candidates use the same eligible cohorts; MLlib pipelines run; probabilities are finite and in [0,1]; final fit uses all designated training rows; distributed/runtime evidence is saved.

## 12. Phase 9 — Metrics, calibration, and scientific comparisons

Predeclare the selection objective: mean PR-AUC over supported configured thresholds on 2021 for hyperparameters and on 2022 for candidate models. For scores within the configured tolerance, prefer lower mean Brier, then the simpler model. If a metric is undefined, report it and use the fixed fallback rule. The selected 2022 score is subject to selection optimism and is not an unbiased generalization estimate. Good ranking does not guarantee good probabilities.

| Output | Rule |
|---|---|
| PR-AUC | Spark BinaryClassificationEvaluator `areaUnderPR` on score/probability; record numBins |
| ROC-AUC | Same cohort; accuracy is not the primary metric |
| Brier | Spark mean `(p-y)^2`; baseline/Brier skill if denominator > 0 |
| Log loss | Clip using a frozen epsilon; Spark aggregation |
| Calibration | Frozen validation bins; mean p, observed frequency, count per bin |
| Precision/Recall/F1 | Fixed 0.5 probability cutoff for description; it is not tuned on the final evaluation period |
| Support | Base/expanded counts, positives/negatives/prevalence/station-time coverage |

Do not call Spark PR-AUC Average Precision unless it has been calculated according to that definition. Record the version and curve approximation; `numBins=0` disables downsampling when cost permits. Mark AUC as N/A for single-class groups; retain rare groups in the table with support/uncertainty ([evaluator API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.BinaryClassificationEvaluator.html)).

Report results per threshold, frozen macro weighting, and station/month strata; pooled thresholds are secondary because they can hide per-threshold performance. Include a prevalence reference on the PR curve; do not claim that Spark's area for a constant predictor always equals prevalence.

Always apply the predeclared Spark IsotonicRegression calibrator fitted on 2023. Do not use calibration ECE to decide whether to enable it. Save raw and calibrated scores plus the calibrator artifact. Do not fit or modify the calibrator on the test set.

For each station/time, score the fixed anchor grid, calibrate each probability, and apply equal-weight PAVA to project onto `p(tau_1) ≥ … ≥ p(tau_6)`. The prediction output uses this projected curve. Interpolate linearly for thresholds inside the inclusive domain and reject values outside it. Report raw, calibrated, and final strict violation rate/max increase. The final rate and maximum increase must both equal zero; verify after projection and interpolation. Store this rule in the frozen spec and inference package.

Ablation A=station+time+threshold; B=A+history+missingness. Use identical training rows/grid and the 2021-frozen family parameters. Report A/B ablation on 2022. Evaluate the frozen winner and climatology once on 2024 using the same eligible cohort. Analyze errors by month/station/threshold/unseen, with onset/continuation defined from history available at `t`.

Observation horizons overlap and storms are temporally dependent. Use paired day/week block bootstrap confidence intervals, resampling all stations/thresholds within the same block; choose block length from train/validation before testing. Do not bootstrap expanded rows independently. If budget is limited, prioritize uncertainty for Brier differences and state which metrics have only point estimates.

**Acceptance:** comparison/threshold tables, calibration, monotonicity, A/B ablation/error analysis; support, excluded samples, and uncertainty are clear.

## 13. Phase 10 — Artifacts, LaTeX, and submission

The model package includes the PipelineModel, calibrator, climatology lookup/fallback, feature schema/config, threshold domain, availability assumptions, training manifest, versions/Git commit, and prediction entry point. Download the entire Spark model directory tree; it is not a single pickle. Include sample queries/expected probabilities/status for unseen/missing/out-of-domain cases.

Load it in a fresh session with the same runtime, run inference through the public station/time/threshold contract, and compare against saved fixtures within a frozen tolerance. Fixtures must contain a small history/metadata sample sufficient for offline execution; real data comes from an input path configured by the user, so a personal bucket does not have to remain available. A successful save does not prove that the model can be reproduced.

### 13.1. Report focus and format

**Follow the two tasks in the Project 2 assignment directly.** Research questions, hypotheses, or a separate research framework are not required. Explain how the data was processed, what patterns Task 1 found, and how well Task 2 predicts probabilities. Use the Project 1 template only as a reference for LaTeX formatting and evidence presentation; do not carry over its MongoDB research content.

Write the report in clear English, with short sentences and consistent terminology. Use **LaTeX and the Project 1 template** once the template source is located. The Project 1 directory and submission ZIP were checked: they contain a PDF, but no `.tex/.cls/.sty` was found. If the source is not provided, reconstruct the layout from the PDF and state that it was reconstructed. Use A4, one column, readable serif font, section numbering, captions, and cross-references. The full Project 1 front matter need not be copied; add a table of contents/list of figures only if useful. Project 2 is individual: list one name and one student ID on the cover/submission; do not copy the Project 1 member list.

Plots use **SciencePlots** (import `scienceplots`) + Matplotlib, with vector PDF where appropriate; check the style's TeX dependencies. Before writing the actual report, read the **academic-writing** skill. The user provided it at `data/academic-writing-SKILL.md`, and it was read on 11/10/2026. Use evidence-based writing; do not invent results or novelty claims. The current scaffold is in `report/main.tex`, with filling instructions in `report/OUTLINE.md`.

No detailed grading rubric is available beyond the requirements in `project_description.txt`; do not invent score weights. **10–12 pages of main content** is a suggestion, not an official page limit; references, AI declaration, and appendix are counted separately. Adjust to the actual results; do not add figures/tables just to fill pages.

| Section | Question to answer | Suggested length |
|---|---|---|
| Introduction | Problem, 2017–2024 data, the two tasks, and scope | 0.5 page |
| Dataset and preprocessing | Source, schema, time period, cleaning, and retained data | 1–1.5 pages |
| Computing environment | Actual run configuration and use of Spark/MLlib | 0.5 page |
| **Task 1: Rainfall patterns** | Within-year and across-year patterns, figures, and interpretation | **2.5–3 pages** |
| **Task 2: Probability prediction** | Target, features, split, models, probability evaluation, and errors | **3–4 pages** |
| Discussion and limitations | Meaning of results, data/model limitations | 0.5–1 page |
| Reproducibility and conclusion | How to rerun, artifacts, and conclusions for both tasks | 0.5 page |

Write the abstract last, at about 150–200 words: objective, data/method, one Task 1 result and one Task 2 result using real measurements, and the main limitation. Do not put illustrative measurements in the submission.

### 13.2. Dataset, cleaning, and runtime environment

Keep it concise, but state the decisions that affect the conclusions so readers can understand where results came from:

- Data source, period used, rainfall units, station identity, and timezone. Explain how observation timestamp differs from update timestamp; cite evidence or assumptions about interval/availability.
- A table by year: raw rows, rows after cleaning, station counts, valid station-days/coverage, and observations excluded by reason. Distinguish missing, invalid, exact duplicate, and conflicting records; count exclusions sequentially to avoid double-counting.
- Explain why missing values are not replaced with zero, how gaps/revisions and station metadata are handled, and how this affects Task 1 and the number of samples with sufficient history/future readings for Task 2.
- **Actual run** configuration: region, master/workers, machine type, vCPU/RAM, disks, Dataproc image, Spark/Python/Java versions, important executor settings. Include processing/training time and evidence of Spark jobs running on workers; do not report planned configuration as a result.
- For **each Spark command or notebook action**, inspect the application in Spark Web UI after its jobs finish; use the live UI if the application remains open, or History Server if it has ended. Save Jobs summary and each Job detail screenshot, Application ID/Job ID, final status, duration, and stages/tasks in the evidence index. State clearly if an application/job failed or if no event log is available to restore the UI.
- Describe the flow raw CSV → cleaned Parquet → aggregates/features → MLlib → metrics/artifacts. Put long logs, Spark plans, and full configuration in the appendix or repo.

### 13.3. Task 1 — Detailed patterns and interpretation

This section must separately cover both **within-year** and **across-year** patterns, as defined in Phase 4. Present them in this order:

1. **Coverage context:** station/year or year/month heatmap and a quality table. State the stations/valid days used in subsequent figures; low coverage can make rainfall totals appear artificially low.
2. **Monthly patterns:** rainfall levels and wet frequency by month with clear denominators/coverage. Choose an aggregation appropriate to Phase 4; do not sum rainfall across stations and call it Singapore's total rainfall. Identify high/low months or periods with measurements, consistency across years, and the effect of missingness.
3. **Hourly patterns:** rainfall/wet frequency by SGT hour over valid observations. Identify hours with more frequent rain or greater rainfall and the practical size of differences; do not infer causes from a figure alone.
4. **Across-year patterns:** compare 2017–2024 using stations with comparable coverage/a common cohort. Identify higher/lower years and check whether conclusions change with coverage policy/cohort; do not claim a long-term climate trend from eight years.
5. **Spatial differences, if useful:** map or station summary with units/support; distinguish location from changes in the station network. Put it in the appendix if it adds no insight to the main requirements.

Add a **distribution** paragraph before monthly patterns: separate valid dry/wet proportions and conditional wet intensity/quantiles, and state pooled weighting. Use complete-day and wet-spell outputs to explain sensitivity or short-term persistence when evidence is available; keep spatial/tail detail in the appendix if it is not needed for the main findings. See Section 7.1 for statistical details and artifacts.

Each results paragraph should include: **measured observation → calculation/cohort → interpretation → relevant limitation**. Refer to figures/tables by name; do not merely describe colors or repeat captions. If comparing rainy seasons/monsoons, cite an official meteorological source and distinguish background context from results calculated from this dataset. Do not assign causal explanations without supporting data.

### 13.4. Task 2 — Detailed write-up from target to probability results

**Problem and target.** State the station/time/threshold inputs and the probability output for cumulative rainfall over the next 30 minutes being **greater than** the threshold. Include the formula `P(R30 > tau | available history)` and a short timeline explaining the six future intervals. State units, boundary convention, label/history eligibility, threshold domain, and handling of missing/unseen/out-of-domain cases. Declare the availability assumption if the work is retrospective prediction only.

**Features and split.** List only features actually used in the feature table: time/station/threshold groups, rainfall history, and missingness; give each group's window, meaning, and rationale. Briefly explain how future information is excluded. The split table lists search training 2017–2020, hyperparameter validation 2021, model selection 2022, full final fit 2017–2022, calibration 2023, and held-out evaluation 2024, with base samples, threshold-expanded samples, positives/prevalence, and purge policy. Do not use the 2024 period to select models/features/calibration.

**Models and training.** Explain climatology, MLlib LR, RF, and GBT; state preprocessing, deterministic 10% hyperparameter screening, full refits of survivors, selected hyperparameters, selection criterion, and the 100% final refit. No training-fraction scaling experiment is run. Put full config in the appendix/repo. Distinguish rainfall threshold `tau` from the fixed descriptive probability cutoff. Explain how class 1 probability is exported and how the full pipeline is saved.

**Main results.** Compare raw winner, isotonic-calibrated winner, PAVA-constrained predictor, and full-training climatology on the same held-out 2024 cohort. Include PR-AUC, ROC-AUC, Brier, support, calibration, and threshold-wise results. State macro/pooled aggregation clearly; mark AUC as N/A for single-class groups and report low support. If there are too many thresholds for a readable table, keep anchor thresholds in the main text and put the full table in the appendix.

**Probability quality.** Include a reliability diagram with predicted probability, observed frequency, and counts per bin; compare raw/calibrated values when correction was applied. Explain ranking and calibration using actual results: a model that discriminates rainfall well does not automatically produce trustworthy probabilities. PR curves use a few primary thresholds frozen before testing, with clear prevalence/support and model labels.

**Features and errors.** Summarize the A/B ablation on 2022 to show the contribution of history/missingness. Briefly analyze major errors by threshold, station/month, onset/continuation, and unseen cases when samples are available. Report raw, calibrated, and constrained monotonicity; the final violation rate and maximum increase must be zero. Explain linear interpolation for unseen in-domain thresholds and rejection outside the domain. Calculate uncertainty as defined in Phase 9; do not call small differences statistically significant without suitable evidence.

**Task 2 conclusion.** State directly which model was selected on 2022, how the fixed final predictor performed on the held-out 2024 evaluation period, under what conditions its probabilities are trustworthy, and where it fails. Disclose that 2024 metrics were observed in v1 and that v1 values were excluded from v2 choices. Do not call 2022 selection scores unbiased validation estimates. If ML does not outperform climatology or calibration does not improve results, report that outcome without changing the scope/cohort to make the model win.

### 13.5. Figures, tables, and report evidence

The plan below guides output selection; it is not a required quota:

| Artifact | Content and data source |
|---|---|
| `quality_by_year.csv` / Table: data quality | Spark cleaning audit, attrition, and coverage by year |
| `coverage_heatmap.pdf` | Aggregated valid coverage; context for Task 1 |
| `monthly_hourly_patterns.pdf` | Two monthly/hourly panels; units, valid denominators, and cohort |
| `annual_patterns.pdf` | Year comparison on the common cohort; sensitivity if needed |
| Table: environment / features | Actual run metadata and frozen feature config |
| Table: model comparison / threshold results | Frozen 2024 metrics, same cohort, available support and uncertainty |
| `pr_curves.pdf` / `calibration.pdf` | Spark score aggregates; primary thresholds, bin counts, and split |
| Table: ablation / error summary | A/B on the same samples and a few failure strata; long table in appendix |
| `report/evidence/spark-ui/` + `index.csv` | Spark History Server screenshots after each Spark command/application: Jobs summary and details for every Job ID created by the command; Application ID, Job ID(s), final status, duration, stages/tasks, and screenshot paths |

Every caption must be self-contained: include time/split, cohort, units, statistic, missingness/coverage rule, and model where relevant. Axes need units/labels, and legends must remain readable on A4; avoid too many lines in one panel. Refer to each figure/table in the text before or near where it appears.

Spark exports **small aggregate CSV/JSON** for tables/curves; do not bring all raw/features into Python for plotting. Each output records `RUN_ID`, split/cohort/config, and artifact source. Generate LaTeX tables/numeric macros from frozen outputs to avoid transcription errors; use a simple manifest to link figures/tables to outputs and commit it. A metric appearing in the abstract, a table, and the conclusion must have the same value, rounding, and cohort.

Record each notebook action that creates a Spark job as one row in the
evidence index. After the action's jobs finish, open **Spark Web UI** (live
UI if the application remains open, Spark History Server if it has ended;
do not use the Dataproc Jobs/YARN page), inspect the Jobs summary and detail page for
every Spark Job ID created by the action, and capture screenshots. Save the original screenshots under
`report/evidence/spark-ui/<RUN_ID>/`; the appendix lists Application ID, Job ID(s),
command/action, final status, duration, and screenshot paths, and embeds the
corresponding images for review.
If an application creates no Spark Job ID, state this and retain the summary screenshot. If a run has not
produced results, leave it clearly marked as not run; do not create illustrative screenshots in place of real evidence.

### 13.6. Discussion, reproducibility, and finalization workflow

The discussion synthesizes both tasks: how patterns inform rainfall understanding and feature choices; how well the model uses history; and how results are limited by rainfall-only inputs, missing data/revisions, a short record, rare heavy rainfall, availability assumptions, threshold domain, and unseen stations. Include only limitations that actually apply. Do not use terms such as “best,” “robust,” “real-time,” or “significant” beyond what has been verified.

For reproducibility, refer to the README, exact runtime/config/commit, manifest, model package, and load/predict command. The conclusion summarizes one or two main findings from each task and usage limitations; do not introduce new results. References include the data source and materials/methods used; every citation must point to a real source, with no placeholders.

The AI declaration accurately lists the tools/models and work they supported (planning/code/debugging/analysis/drafting/editing), following the assignment template; do not claim results were verified unless they were checked. The PDF includes names/student IDs/references/captions.

Writing workflow:

1. Write the dataset/cleaning/environment/target/split sections once confirmed, alongside data processing; when results are unavailable, keep the draft clearly marked and do not guess values.
2. Complete Task 1 from aggregates and Task 2 from frozen evaluation outputs; write interpretation after each figure/table.
3. Write the discussion/conclusion/abstract last; remove repeated passages and theory that does not serve the assignment.
4. Compile LaTeX and fix references/overflow; render and read every page at A4 size. Check figures, fonts, captions, table widths, and page breaks.
5. Map every assignment requirement to a section/page: cleaning, within/across-year patterns, features, model, environment/VM config, performance, limitations/insights, and AI declaration. Check measurements against outputs and remove all TODOs/placeholders/broken references from the submission.
6. Reconcile every Spark command/notebook action against live Spark Web UI or History Server and the evidence index: each action has a Jobs summary screenshot; every Job ID has a detail screenshot; IDs/statuses match the logs and outputs; no failed/canceled run is missing.

README: prerequisites/sample config, data source/manifest/upload, notebook order, batch commands, versions/lifecycle/artifact locations, and load/predict. The package contains the source and model needed for assessment; it does not depend on a personal bucket remaining accessible.

```text
DSA5208_Project2_StudentID/
├── README.md
├── requirements-local.txt
├── requirements-cluster.txt
├── config/                 # examples + experiment config, no credentials
├── notebooks/
├── src/
├── scripts/
├── tests/
├── models/                 # pipeline/calibrator/lookup + metadata
└── report.pdf
```

Keep LaTeX source, figures, and aggregate tables in the repo to reproduce the report. Do not package raw/cache/temp outputs/credentials. Check the Canvas size limit and extract/load the model from the submission archive itself.

**Acceptance:** PDF meets all requirements; README runs; archive contains the required model/code/config/fixtures; reload and inference succeed in a fresh session.

## 14. Phase 11 — Shutdown, final QA, and timeline

Before shutdown: export notebooks, persist processed/features/model/results/run manifest on GCS, and reconcile artifacts. Then use the shutdown cell in `00_environment.ipynb` to delete the cluster created by this run and query the project/region cluster list again; retain confirmation output in the notebook.

Confirm that the cluster is gone and check remaining VMs/disks/supporting resources/billing. GCS still incurs storage charges: download and reconcile model/results/submission locally before deleting unneeded cloud copies; set retention within the credit budget and before expiry, and do not keep the bucket indefinitely. A notebook that is no longer visible does not prove that compute was deleted.

Final checklist:

- [ ] The verified Billing Account ID contains credit `3LU63BPGY77JFDQQ`; the project is linked only to this account, with no other credit/billing account used.
- [ ] Credit ledger, pending costs, and remaining balance have been checked; no paid fallback is used if the credit is insufficient/expired.
- [ ] Manifest includes only 2017–2024; hashes and paths are reproducible.
- [ ] Manifest bytes/rows/schema/timestamps are reconciled; the unknown original retrieval time is clearly stated.
- [ ] Schema/interval semantics/availability/quality policy have evidence.
- [ ] Missing values are not treated as zero; duplicates/conflicts/gaps are deterministic and auditable.
- [ ] Label uses the correct 30-minute horizon/strict greater-than rule and does not cross a split/fold.
- [ ] Learned preprocessing/model/calibration are fit on the correct temporal subsets.
- [ ] Task 1 has within-year/across-year findings and coverage/bias sensitivity.
- [ ] Inference accepts station/time/threshold; domain/status are clear.
- [ ] Climatology/LR/GBT, per-threshold metrics, and calibration assessment are complete.
- [ ] Monotonicity, A/B ablation, onset/continuation/unseen analysis have evidence.
- [ ] Manifest is frozen before 2024; post-test exploration is declared.
- [ ] Spark processing/MLlib and distributed/runtime evidence are saved.
- [ ] Every notebook Spark action has Spark Web UI screenshots after terminal state: Jobs summary and each Job detail; Application ID/Job ID, status, duration, stages/tasks match the evidence index, log, and output.
- [ ] Notebook restart-and-run-all uses the same code/config and saves outputs.
- [ ] Reload/inference succeeds from the submission package in a fresh session.
- [ ] PDF has accurate names/IDs/captions/limitations/sources/AI declaration.
- [ ] Package contains no credentials/raw/cache/hard-coded personal paths.
- [ ] Cluster is deleted, artifacts are persisted, and remaining resources/costs are checked.

| Date (SGT) | Target output |
|---|---|
| 11/10 | Review; manifest/schema/time semantics; cloud preflight |
| 12/10 | Cloud smoke; quality Parquet/environment evidence; draft data/cleaning/environment |
| 13/10 | Task 1 training-year analysis; labels/features/leakage fixtures; draft Task 1/target/split |
| 14/10 | Three-model pilot, temporal selection/cost evidence |
| 15/10 | Full finalists; A/B ablation; calibration holdout fit |
| 16/10 | Freeze; final 2024 evaluation; Task 1 full-period results |
| 17/10 | LaTeX/README/package/model reload validation |
| 18/10 | Error/repackaging buffer; submit before 23:59 |

Historical timeline above describes v1. For v2, track completion in `data/TASK.md`; do not claim a full-data fit unless the saved run evidence confirms the actual 2017–2022 row count and 100% path. Calibration, monotonicity projection, and A/B ablation are part of the active protocol.

**Acceptance:** final checklist has evidence; cluster/supporting resources are checked after shutdown; package is submitted by the deadline and the receipt is saved.

## Execution update — 11/10/2026

- Active project: `sg-rainfall-dsa5208-261011`, linked to the correct Education account.
- All 8 CSVs and the manifest were uploaded; SHA256 custom metadata matched. Notebook 01 independently checked cloud byte-level hashes.
- Quota `SSD_TOTAL_GB=250`: the cluster used a 100 GB master disk + 75 GB balanced disks on each of two workers, replacing a 300 GB request rejected by the API. Compute remained 3×e2-standard-4, image 2.2.87-debian12, private IP/IAP/Jupyter/event logging, 45-minute idle timeout / 3-hour maximum lifetime.
- `07_cloud_execution.ipynb` launches Jupyter kernels on the master and retrieves checkpointed notebooks; `08_cloud_diagnostics.ipynb` saves read-only diagnostics; `09_refresh_source.ipynb` uploads the source bundle/hash before a new application. Do not mark EDA/model work complete without actual outputs.
- Spark retains up to 10,000 jobs/stages/executions to avoid losing job information during multi-step actions. Every Spark action still requires review/capture of the real UI.
- Notebook 04 ran in `application_1791706788393_0005` starting at 10:15:53 UTC. The live Spark Jobs UI at 11:15 UTC showed 1,879 completed job records and Job 1879 active; model directory `gbt_A/` had been written, but `gbt_B/` and `models/frozen_manifest/` were absent. The cluster reached hard-delete at 11:17:32 UTC; at 11:17:58 UTC Dataproc status was `DELETING` and SSH/IAP to the master had disconnected. YARN final state could not be queried after the master stopped; therefore, Notebook 04 was recorded as **incomplete**, not marked succeeded/failed without a final application record. Do not run Notebook 05 or read test metrics before a frozen manifest exists.
- The cluster was `DELETING` at 11:18:17 UTC and no longer appeared in the project/region listing at 11:19:32 UTC; the Dataproc active-job listing was also empty. Because SSH/master was lost before querying final YARN state, there was no terminal YARN record to classify the action as success or Spark failure; record it as incomplete due to hard lifetime. Do not extend/create another cluster until the user signs in to Console to check the Education credit balance, pending costs, and conditions for continuing. The runtime used credit ID/billing account `3LU63BPGY77JFDQQ` / `01DAD8-AE5F1A-E3910A`; the initial USD 50 snapshot is not the current balance.
- App5's Jobs summary and Job 1848 detail were opened directly in Spark Jobs UI; Job 1848 showed `SUCCEEDED`, 1 second, two stages, and 21/21 tasks in each stage. A JPEG summary screenshot was saved from the Spark UI tab still open in the browser at `report/evidence/spark-ui/20261011-full-01/app5-jobs-summary-active-checkpoint.jpg`. It was saved at 14:13:22 UTC from a previously rendered page, not a fresh load; the page showed 1,879 completed jobs and Job 1879 active after 36 seconds, with 0/1 stage. Accessibility state at observation showed 2/8 tasks, but the tasks column was cut off in the screenshot viewport. When opening the detail later, the local Spark UI proxy returned `ERR_CONNECTION_REFUSED`; this retained checkpoint does not prove final/YARN state. App5 Job detail screenshots and app4/app3/app1 summary/detail screenshots were still missing; the summary and each Job detail screenshot requirement had not been met, and the Spark History API is not a substitute.
- Editorial pass on `report/main.tex`: the Abstract and Conclusion were replaced with measured Task 1 findings and an accurate incomplete status for Task 2; unmeasured final-test metrics are explicitly unavailable, and no editorial prompt fields or fake `[Result]` placeholders remain. LaTeX compiled successfully. The report was still not ready for submission because it lacked model freeze/test, cleaning reconciliation, 2024 post-freeze EDA, and real Spark UI screenshots.
- Review of `fit_models()`/`threshold_metrics()` after app5 found that each metric calculation made separate scans to check invalid scores and discover thresholds even though the notebook already knew the grid. Validity checks were combined into the metric aggregate and the declared grid was passed in to remove the redundant discovery step. This is a static code optimization; it was not rerun on Spark, so no runtime/job-count reduction is claimed. Measure it in the next authorized run.
- Review of the EDA report against the exported aggregate CSVs reconfirmed that coverage, wet frequency, P99/exceedance, monthly extrema, hourly peak/range, and common-cohort values matched after rounding. The app5 Spark Jobs checkpoint screenshot described above was saved; Job detail could not be opened because the proxy refused the connection after the cluster stopped. No new compute was created.
- To prepare a safe retry after the billing gate passes, Notebook 04 uses prefix `models/attempt-02/`, checks that it does not exist, and preserves app5's incomplete `models/` directory. Notebook 05 uses the same attempt ID, verifies config/model root from the frozen manifest before reading test rows, and refuses to overwrite an existing final-evaluation directory. If the next attempt is incomplete, retain its artifacts and move both notebooks to a new attempt ID; this change has not been run on the cluster.
- Correction to the Console check at 15:26 UTC on 11/10/2026: USD 50.00/100% appeared only in an old Credits tab opened before the run; it was not refreshed and therefore did not confirm the post-run balance. Fresh navigation/reload to Credits, Reports, and the Cost table for the correct Billing Account for Education `01DAD8-AE5F1A-E3910A` showed blank pages; the current balance, cost line items, and pending usage were unverified. No new compute was started; keep the cloud gate closed until these details can be read along with storage/retention costs.
- A later read-only gcloud recheck found no Dataproc cluster, Compute VM, persistent disk, or reserved address in the project/region; bucket `gs://sg-rainfall-dsa5208-261011-data` in `ASIA-SOUTHEAST1` retained 11.43 GiB. This is storage size, not a calculated cost; balance, Cost table, pending charges, and storage charges were unverified. At the next Console open/reload, the Credits page remained blank.
- The gate was updated for future runs: `src/rainfall/billing.py` checks the correct Education account + credit ID, eligible credit list, three Credits/Reports/Cost table pages, timestamp freshness, usage/pending charges, gross USD 40 limit, and USD 10 reserve; each session is estimated at up to USD 8. Notebook 00 defaults to `CREDIT_LIVE_VERIFIED=False`; older runtime manifests lack a fresh snapshot, and cloud notebooks fail closed. The gate was not re-enabled and no compute was created.
- `notebooks/11_spark_ui_evidence.ipynb` was changed so `index.csv` has one row per notebook action, lists Job IDs/status/duration/stages/tasks, and leaves screenshots pending until real Spark Web UI pages are captured. API metadata is stored separately; this change has not been rerun and does not supply the missing screenshots.
- Recheck on 12/10/2026: Chrome showed the Billing Account for Education Credits page with credit `3LU63BPGY77JFDQQ` at 96% / USD 47.91 remaining of USD 50.00. This USD 2.09 account-level decrease does not prove which costs this project incurred. Reports/Cost table/pending usage were not verified, so the cloud gate remains closed; no new cluster or Spark job was created.
