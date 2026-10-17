# Project Memory: DSA5208 Singapore Rainfall

Updated: 12/10/2026 (Asia/Singapore). This note restores context when work resumes. The cloud/cost states below are dated snapshots and must be rechecked before running work or making claims about current state.

**Continuation rule:** before carrying out the next project request, reread this file together with `data/TASK.md`. Use the decisions below as the default context and do not ask again about questions that have already been answered; recheck cloud state that can change over time. Update the status section when new evidence or decisions become available. When the user asks to "continue" or "do everything," keep working through the open TASK checkboxes until evidence is available; do not stop at writing a plan/TASK if independent implementation steps remain that can be completed safely within the agreed constraints.

## Goals and scope

- Individual DSA5208 Project 2: Singapore Rainfall Analysis and Prediction.
- Follow the two assignment tasks: Task 1 analyzes rainfall patterns within and across years; Task 2 predicts the probability that cumulative rainfall at a station over the next 30 minutes will be **greater than** a threshold.
- Do not invent research questions, novelty claims, or rubric weights. Project 1 is a reference for presentation only.
- The report carries substantial assessment weight: prioritize evidence-based writing with real measurements and clearly stated denominators/cohorts/limitations. Write the abstract last, after final results are available.
- The assignment description gives a deadline of 18/10/2026 23:59; confirm the timezone and submission limits in Canvas.

## Decisions already made

- The user chose to interpret `timestamp` as the **end of a 5-minute measurement interval**. This is an accepted project assumption, not a confirmation from NEA. Classify the results as retrospective event-time prediction while the semantics/availability of the update fields remain unverified; do not claim real-time performance.
- The dataset consists of exactly eight annual CSVs, 2017–2024; do not use 2016 as warm-up data. Keep raw data immutable outside the repository. The manifest is at `manifests/input_manifest.json`; the original retrieval time is unknown.
- Project code must be invoked/run through Jupyter Notebooks so outputs appear in the notebooks. Local setup notebook 00 can run on the Mac; Spark processing/model training runs on Dataproc Jupyter, without creating a local Spark cluster. Reusable functions in `src/`/`scripts/` are imported by notebooks.
- For each Spark action, open Spark Jobs UI/History Server after it reaches a terminal state and save a real Jobs summary screenshot and a screenshot for each Job detail. Dataproc Jobs/YARN/API metadata do not replace screenshots. Record actions that create no jobs as no-jobs.
- Use only cloud project `sg-rainfall-dsa5208-261011`, region `asia-southeast1`, Billing Account for Education `01DAD8-AE5F1A-E3910A`, and credit ID `3LU63BPGY77JFDQQ`. Do not switch accounts, use another credit, add a payment method, or use a paid fallback.
- The user asked to continue without waiting for Reports/Cost table or asking again about the cost. Credits are applied automatically at billing-account scope; gcloud does not pin a Credit ID to each resource/job. The code gate must verify the project/account linkage and the user-authorized credit ID, without requiring fabricated cost/pending values.

## Cloud status checked on 12/10/2026

- The active `gcloud` identity is `damminhtienchl@gmail.com`.
- Project `sg-rainfall-dsa5208-261011` is ACTIVE, `billingEnabled=true`, and linked to account `billingAccounts/01DAD8-AE5F1A-E3910A`.
- The billing account list also contains `My Billing Account`, but this project is linked to the correct Education account; never link the other account.
- The Dataproc cluster list in `asia-southeast1` was empty at the time of inspection.
- Bucket `gs://sg-rainfall-dsa5208-261011-data`: location `ASIA-SOUTHEAST1`, STANDARD, uniform bucket access, approximately **11.43 GiB** at inspection time; deleting the cluster does not delete GCS storage.
- The browser Console showed the correct Education account and credit `3LU63BPGY77JFDQQ`: **96%, USD 47.91 of USD 50**. This is the account-level credit balance at the time shown, not a cost attributed to this project.
- No active Dataproc cluster appeared in this snapshot. Before any further cloud operation, recheck the billing link, resource inventory, and bucket prefixes.

## Key data and protocol

- Manifest inventory: 48,538,713 rows, 8,047,044,701 bytes; all eight files share a 12-field schema and `+08:00` timestamps; original retrieval time is unavailable.
- Exact timestamp alignment to 300 seconds is currently configured as `alignment_tolerance_seconds=0`. Records in 2017 begin at `08:04:59`; do not automatically floor/round them to the grid.
- Cleaning policy uses raw provenance and an explicit schema; rainfall must be finite/nonnegative, with unit mm and the correct type; separate exact duplicates; quarantine conflicting `(station_id, epoch)` keys; never convert missing values to zero; reconcile raw = retained + disjoint excluded reasons exactly for each year.
- Target: `R30(s,t)=sum(r(s,t+5k min), k=1..6)` and `Y=1[R30>threshold_mm]`; use the interval-end convention; an exact threshold match is negative; all six contiguous future intervals are required, with no gaps.
- Temporal split: train 2017–2022; selection Jan–Jun 2023; calibration Jul–Dec 2023; final test 2024; purge label endpoints that cross a split. 2024 label-valid counts were viewed before freeze, but according to the old log, prevalence/model scores were not viewed; do not describe the evaluation as fully blind.
- `config/experiment.json`: training thresholds `[0.2,0.5,1,2,5,10]` mm; unseen thresholds `[0.3,0.75,1.5,3.5,7.5]`; domain `[0.2,10]`; lags 5/10/15/30/60 min; rolling windows 15/30/60/180 min.
- Candidate training is currently configured with a 0.5% pilot fraction, a 2% final ML training fraction, and seed 5208; LR `regParam` `{0.001,0.01,0.1,1}`, `maxIter` 50; GBT `(depth,iter)` `{(3,20),(4,20),(3,40),(4,40)}`, `maxBins64`. The climatology baseline uses all eligible training data, so its sample scope differs from LR/GBT.
- Calibration: selection ECE trigger 0.01; if enabled, pooled isotonic fit on the calibration holdout; 10 reliability bins; probability cutoff grid `{0.05,0.1,0.2,0.3,0.5}`; 1000 paired calendar-week bootstrap replicates, seed5208.
- Current Spark config: image `2.2.87-debian12`; master + 2 workers `e2-standard-4`; 100GB master / 75GB worker balanced disks; driver/executor request 4g; 4 executors × 2 cores, dynamic allocation off; 64 shuffle partitions; UI retains 10,000 jobs/stages/SQL executions. The prior listener OOM/hard-delete warrants checkpointing and measurement; it is not evidence that the current runtime completed.

## Evidence available and unfinished work

- `report/tables/pretest/` contains Task 1 aggregate CSVs and `findings.json` for 2017–2023 only. Findings: there is no annual common-station cohort even at 90% coverage; a controlled annual comparison across all eight years is unavailable.
- Previous feature notebook `notebooks/executed/03_task2_features-attempt-1.ipynb` completed target fixtures and materialized base features; its outputs are historical run evidence, not proof that the current resources/artifacts are present.
- Previous cleaning output `cleaned/` had `_SUCCESS`, but the cleaning action timed out/listener OOM before rejected/exclusion/alignment/quality/coverage outputs and reconciliation were produced. Do not call cleaning complete just because `_SUCCESS` exists.
- Previous model run application `application_1791706788393_0005` ended at the Dataproc 3-hour hard delete while Job 1879 was still active; `gbt_A` exists but `gbt_B` and the frozen manifest were absent from the record. No final model freeze or 2024 final metrics has been verified.
- Previous screenshots: an app5 Jobs-summary active checkpoint exists, but app5 Job detail and app1/app3/app4 full summary/details were missing from the evidence files. The screenshot requirement remains open.
- `report/main.tex` compiled in an earlier turn, but it is not final without cleaning reconciliation, model freeze/test results, eight-year EDA, complete Spark UI screenshots, package reload, and submission receipt.
- The prior notebook run `RUN_ID=20261011-full-01` has many artifacts in the GCS bucket. Keep it immutable; inspect each output/metadata before reuse. A fresh retry must use a distinct run ID and output prefix.

## Most recent continuation checkpoint: 12/10/2026

- `data/TASK.md` is the current checklist. Section A (synchronizing the billing policy and run-scoped paths) was completed in commit `9392908` (`Fix cloud billing gate and version run artifacts`). An earlier separate commit `c351f97` added the initial memory note.
- The next planned run is `RUN_ID=20261012-full-01`, `MODEL_ATTEMPT=attempt-01`; the new runtime/source bundle prefix was verified empty at the latest check. Keep run `20261011-full-01` immutable.
- `notebooks/00_environment.ipynb` and analysis notebooks 01–05 were updated to use the manifest/source bundle for the new run; support notebooks 07–11 were also synchronized. Static checks passed: `py_compile` for the billing helper, JSON/AST checks for notebook code cells, and a review of old cost gates.
- Notebook 00 has not been run, no new-run cluster has been created, and there are no new results for sections B through F. Do not treat these static checks as evidence that the pipeline ran.
- Next implementation step: confirm the correct `00_environment.ipynb` window/tab in VS Code, select the local Python 3.14 kernel for the setup notebook, run cells in order, and inspect each output. Before any resource-creation cell, recheck live identity, project/account link, and inventory; run Spark workloads only on Dataproc Jupyter.
- At the last check, the VS Code UI showed a mixture of old/new notebook tabs; confirm the tab name and kernel in the UI before running anything to avoid running the wrong notebook.
- Git already had changes outside the scope of this note (`README.md`, `report/main.tex`, three `src/rainfall` modules, `.DS_Store`, `report/evidence/`, `report/figures/`, `report/tables/`). Do not stage/revert these items when updating memory.

## Working conventions

- The current task checklist is `data/TASK.md`; detailed original protocol/history is in `data/TASK_SPEC.md`.
- Read project `AGENTS.md` instructions before changes. Prefer focused edits, preserve pre-existing dirty/untracked user artifacts, and commit only explicit relevant paths after inspecting diff and running `git diff --check`.
- Do not present planned parameters as selected model settings or partial traces as completed results. Verify current state from files/notebook outputs/Cloud APIs before relying on this memory.
