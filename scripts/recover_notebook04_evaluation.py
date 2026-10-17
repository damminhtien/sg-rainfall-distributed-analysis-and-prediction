"""Resume Notebook 04 final evaluation from its saved, frozen artifacts."""

import sys

try:
    import IPython.display
except ImportError:
    import types
    _ipython = types.ModuleType('IPython')
    _display_module = types.ModuleType('IPython.display')
    def display(value):
        print(value)
    _display_module.display = display
    _ipython.display = _display_module
    sys.modules['IPython'] = _ipython
    sys.modules['IPython.display'] = _display_module

# Notebook code cell 1
import hashlib
import json
import os
import random
import subprocess
import sys
import tempfile
import time
from functools import reduce
from pathlib import Path

from IPython.display import display
from pyspark import StorageLevel
from pyspark.ml.regression import IsotonicRegression
from pyspark.sql import SparkSession, Window, functions as F

BUCKET = os.environ.get("BUCKET", "sg-rainfall-dsa5208-261011-data")
RUN_ID = os.environ.get("RUN_ID", "20261016-v2-fixed8-04")
ROOT = f"gs://{BUCKET}/runs/{RUN_ID}"
MODEL_ROOT = f"{ROOT}/models/final_v2"
RESULTS_ROOT = f"{ROOT}/results"


def gcloud(args):
    return subprocess.run(
        ["gcloud", *args], check=True, capture_output=True, text=True
    )


def read_gcs_text(uri):
    return gcloud(["storage", "cat", uri]).stdout


config_raw = read_gcs_text(f"{ROOT}/config/experiment.json")
source_hashes_raw = read_gcs_text(f"{ROOT}/config/source_hashes.json")
input_manifest_raw = read_gcs_text(f"{ROOT}/config/input_manifest.json")
frozen_spec_raw = read_gcs_text(f"{ROOT}/artifacts/frozen_spec.json")
config = json.loads(config_raw)
source_hashes = json.loads(source_hashes_raw)
frozen_spec = json.loads(frozen_spec_raw)

config_sha256 = hashlib.sha256(config_raw.encode("utf-8")).hexdigest()
source_hashes_sha256 = hashlib.sha256(source_hashes_raw.encode("utf-8")).hexdigest()
input_manifest_sha256 = hashlib.sha256(input_manifest_raw.encode("utf-8")).hexdigest()
frozen_spec_sha256 = hashlib.sha256(frozen_spec_raw.encode("utf-8")).hexdigest()

assert config["protocol_version"] == 2
assert frozen_spec["protocol_version"] == 2
assert frozen_spec["experiment_config_sha256"] == config_sha256
assert frozen_spec["source_hashes_manifest_sha256"] == source_hashes_sha256
assert frozen_spec["input_manifest_sha256"] == input_manifest_sha256
assert frozen_spec["final_training_fraction"] == 1.0
assert frozen_spec["selection_sample_fraction"] == config["model_selection_sample_fraction"]
assert frozen_spec["calibration"]["sample_fraction"] == config["calibration_sample_fraction"]
assert frozen_spec["calibration"]["sampling_seed"] == config["sampling_seed"]
assert frozen_spec["final_training_partitions"] == [
    "search_train_2017_2020", "hp_validation_2021", "model_selection_2022"
]
assert frozen_spec["thresholds_mm"] == config["thresholds_mm"]
assert frozen_spec["calibration"]["method"] == "isotonic"
assert frozen_spec["calibration"]["always_applied"] is True
assert frozen_spec["monotonic_projection"]["method"] == "equal_weight_pava_nonincreasing"
assert frozen_spec["threshold_interpolation"]["method"] == "linear"
assert frozen_spec["threshold_interpolation"]["outside_domain"] == "reject"

module_path = Path(tempfile.gettempdir()) / "dsa5208-rainfall-v2" / "rainfall_modeling.py"
module_path.parent.mkdir(parents=True, exist_ok=True)
gcloud(["storage", "cp", f"{ROOT}/source/rainfall_modeling.py", str(module_path)])
module_sha256 = hashlib.sha256(module_path.read_bytes()).hexdigest()
assert module_sha256 == frozen_spec["modeling_source_sha256"]
assert module_sha256 == source_hashes["scripts/rainfall_modeling.py"]


def notebook_source_sha256(uri):
    local = module_path.parent / Path(uri).name
    gcloud(["storage", "cp", uri, str(local)])
    notebook = json.loads(local.read_text(encoding="utf-8"))
    source_only = {
        "cells": [
            {
                "cell_type": cell["cell_type"],
                "source": (
                    "".join(cell.get("source", []))
                    if isinstance(cell.get("source", ""), list)
                    else cell.get("source", "")
                ),
            }
            for cell in notebook["cells"]
        ]
    }
    canonical = json.dumps(
        source_only, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


for relative_path in (
    "notebooks/02_build_features.ipynb",
    "notebooks/03_model_development.ipynb",
    "notebooks/04_final_fit_evaluate.ipynb",
):
    actual_hash = notebook_source_sha256(f"{ROOT}/source/{relative_path}")
    assert actual_hash == source_hashes[relative_path], relative_path
    frozen_key = {
        "notebooks/02_build_features.ipynb": "feature_source_sha256",
        "notebooks/03_model_development.ipynb": "development_notebook_sha256",
        "notebooks/04_final_fit_evaluate.ipynb": "final_notebook_sha256",
    }[relative_path]
    assert actual_hash == frozen_spec[frozen_key], relative_path

sys.path.insert(0, str(module_path.parent))
from rainfall_modeling import (
    build_pipeline,
    climatology_scores,
    deterministic_sample,
    expand_thresholds,
    fit_climatology_tables,
    interpolate_thresholds,
    mark_unseen,
    monotonicity_summary,
    probability_metrics,
    project_nonincreasing,
    score_model,
    threshold_metrics,
)

spark = SparkSession.builder.appName("dsa5208-rainfall-v2-final").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", str(config["shuffle_partitions"]))
spark.conf.set("spark.sql.session.timeZone", config["timezone"])
spark.conf.set("spark.sql.ansi.enabled", "false")
actual_spark_conf = {
    name: spark.conf.get(name)
    for name in (
        "spark.sql.shuffle.partitions",
        "spark.sql.session.timeZone",
        "spark.sql.ansi.enabled",
    )
}
assert actual_spark_conf["spark.sql.shuffle.partitions"] == str(config["shuffle_partitions"])
assert actual_spark_conf["spark.sql.session.timeZone"] == config["timezone"]
assert actual_spark_conf["spark.sql.ansi.enabled"] == "false"
display({
    "run_id": RUN_ID,
    "selected_model": frozen_spec["selected_model"],
    "frozen_spec_sha256": frozen_spec_sha256,
    "config_sha256": config_sha256,
    "input_manifest_sha256": input_manifest_sha256,
    "modeling_source_sha256": module_sha256,
    "spark_conf": actual_spark_conf,
})

# Recover only the final evaluation from the successfully saved frozen artifacts.
import ast
import importlib.util

from pyspark.ml import PipelineModel
from pyspark.ml.regression import IsotonicRegressionModel

# Verify and import the corrected PAVA implementation separately. The parent
# run's frozen module is still checked above and remains immutable.
recovery_module_path = module_path.parent / "rainfall_modeling_recovery.py"
PARENT_RESULTS_ROOT = RESULTS_ROOT
RESULTS_ROOT = f"{PARENT_RESULTS_ROOT}/recovery/attempt-03"
gcloud([
    "storage", "cp",
    f"{ROOT}/source/recovery/attempt-03/rainfall_modeling.py",
    str(recovery_module_path),
])
recovery_module_sha256 = hashlib.sha256(recovery_module_path.read_bytes()).hexdigest()
recovery_module_spec = importlib.util.spec_from_file_location(
    "rainfall_modeling_recovery", recovery_module_path
)
recovery_module = importlib.util.module_from_spec(recovery_module_spec)
recovery_module_spec.loader.exec_module(recovery_module)
project_nonincreasing = recovery_module.project_nonincreasing
interpolate_thresholds = recovery_module.interpolate_thresholds

# Exercise both repaired Spark expressions on a tiny synthetic curve before
# reading the 2024 holdout.
synthetic_probabilities = [0.2, 0.8, 0.6, 0.9, 0.2, 0.1]
synthetic_scores = spark.createDataFrame(
    [
        (
            "synthetic",
            0,
            recovery_module.threshold_units(
                threshold, config["rainfall_integer_scale"]
            ),
            float(threshold),
            float(probability),
        )
        for threshold, probability in zip(
            config["thresholds_mm"], synthetic_probabilities
        )
    ],
    "station_id string, epoch long, threshold_units long, threshold_mm double, p double",
)
synthetic_projected = project_nonincreasing(
    synthetic_scores,
    config["thresholds_mm"],
    rainfall_integer_scale=config["rainfall_integer_scale"],
)
synthetic_monotonicity = monotonicity_summary(
    synthetic_projected, probability_col="projected_p", tolerance=0.0
)
if synthetic_monotonicity["strict_violation_rate"] != 0.0:
    raise AssertionError(
        f"Synthetic PAVA projection is not monotone: {synthetic_monotonicity}"
    )
synthetic_requests = spark.createDataFrame(
    [("synthetic", 0, value) for value in (0.35, 0.75, 1.5, 7.5)],
    "station_id string, epoch long, threshold_mm double",
)
synthetic_interpolated = interpolate_thresholds(
    synthetic_projected,
    synthetic_requests,
    config["thresholds_mm"],
    output_col="p",
    requested_thresholds=[0.35, 0.75, 1.5, 7.5],
)
synthetic_values = [
    row["p"] for row in synthetic_interpolated.orderBy("threshold_mm").collect()
]
if len(synthetic_values) != 4 or any(
    left < right for left, right in zip(synthetic_values, synthetic_values[1:])
):
    raise AssertionError(
        f"Synthetic in-domain interpolation is not monotone: {synthetic_values}"
    )
print({
    "pava_smoke_monotonicity": synthetic_monotonicity,
    "interpolated_smoke_probabilities": synthetic_values,
})

# Reuse the persisted final GBT, calibrator and climatology tables. These were
# successfully written before the parent Notebook 04 failed at PAVA.
selected_name = frozen_spec["selected_model"]
selected_model = PipelineModel.load(f"{MODEL_ROOT}/selected_pipeline")
calibrator = IsotonicRegressionModel.load(f"{MODEL_ROOT}/isotonic_calibrator")
climatology_root = f"{MODEL_ROOT}/climatology"
final_climatology_tables = {
    name: spark.read.parquet(f"{climatology_root}/{name}")
    for name in ("global", "month_hour", "station_hour")
}

# Recover the row counts and calibration diagnostics from the preserved parent
# execution log; fail closed if the expected checkpoints are not present.
parent_log = read_gcs_text(
    f"{ROOT}/results/overnight_execution/logs/notebook_04_final_fit_evaluate.log"
)
fit_line = next(
    line for line in parent_log.splitlines()
    if line.startswith("{'selected_model': '")
)
fit_summary = ast.literal_eval(fit_line)
calibration_line = next(
    line for line in parent_log.splitlines()
    if line.startswith("{'partition': 'calibration_2023'")
)
calibration_summary = ast.literal_eval(calibration_line)
final_training_base_rows = int(fit_summary["valid_2017_2022_base_rows"])
final_training_expanded_rows = int(
    fit_summary["threshold_expanded_final_training_rows"]
)
model_fit_seconds = float(fit_summary["model_fit_seconds"])
if fit_summary["selected_model"] != selected_name:
    raise ValueError("Persisted model identity and parent fit log do not match.")
development_summary = json.loads(
    read_gcs_text(f"{PARENT_RESULTS_ROOT}/development_summary.json")
)
if final_training_base_rows != sum(
    int(value)
    for key, value in development_summary["valid_base_rows"].items()
    if key in {"search_train_2017_2020", "hp_validation_2021", "model_selection_2022"}
):
    raise ValueError("Parent fit row count does not match the frozen valid-row counts.")

# StringIndexer was fitted on exactly the valid full-training station IDs, so
# its labels provide the same seen-station set without rescanning 34.5M rows.
station_indexer = selected_model.stages[0]
if station_indexer.getInputCol() != "station_id" or not hasattr(station_indexer, "labels"):
    raise ValueError("Saved pipeline does not expose the frozen station indexer.")
station_labels = station_indexer.labels
if len(station_labels) != len(set(station_labels)):
    raise ValueError("Saved station indexer labels are not unique.")
training_stations = spark.createDataFrame(
    [(label,) for label in station_labels], "station_id string"
).persist(StorageLevel.DISK_ONLY)

# Keep the cleanup block from Notebook 04 valid without materializing another
# copy of the full training table.
training_partitions = []
final_training_base = spark.range(0).persist(StorageLevel.DISK_ONLY)


def reliability_table(scores, probability_col="p"):
    bins = int(config["calibration_bins"])
    return (
        scores.withColumn("_probability", F.col(probability_col))
        .withColumn(
            "bin",
            F.least(
                F.floor(F.col("_probability") * bins), F.lit(bins - 1)
            ).cast("int"),
        )
        .groupBy("threshold_mm", "bin")
        .agg(
            F.avg("_probability").alias("predicted_probability"),
            F.avg("label").alias("observed_frequency"),
            F.count("*").alias("n"),
        )
    )


def weighted_ece(scores, probability_col="p"):
    bins = reliability_table(scores, probability_col)
    return bins.withColumn(
        "weighted_error",
        F.abs(F.col("predicted_probability") - F.col("observed_frequency"))
        * F.col("n"),
    ).agg((F.sum("weighted_error") / F.sum("n")).alias("ece")).first()["ece"]



# Notebook code cell 4
# Reuse the complete anchor predictions written by recovery attempt 02.
test_base = (
    spark.read.parquet(f"{ROOT}/base_features/split=test")
    .where(F.col("label_valid"))
    .persist(StorageLevel.DISK_ONLY)
)
test_base_rows = test_base.count()
if test_base_rows == 0:
    raise ValueError("The held-out evaluation partition is empty.")

test_anchor_queries = mark_unseen(
    expand_thresholds(test_base, config), training_stations
).persist(StorageLevel.DISK_ONLY)
final_test_scores = spark.read.parquet(
    f"{PARENT_RESULTS_ROOT}/scores/heldout_2024_anchor_predictions"
).persist(StorageLevel.DISK_ONLY)
expected_anchor_rows = test_base_rows * len(config["thresholds_mm"])
actual_anchor_rows = final_test_scores.count()
if actual_anchor_rows != expected_anchor_rows:
    raise ValueError(
        f"Saved heldout anchors are incomplete: {actual_anchor_rows} != {expected_anchor_rows}."
    )

raw_test_scores = final_test_scores.withColumn("p", F.col("raw_p")).persist(
    StorageLevel.DISK_ONLY
)
calibrated_test_scores = final_test_scores.withColumn(
    "p", F.col("calibrated_p")
).persist(StorageLevel.DISK_ONLY)
projected_test_scores = final_test_scores.withColumn(
    "projected_p", F.col("p")
).persist(StorageLevel.DISK_ONLY)
baseline_test_scores = climatology_scores(
    test_anchor_queries, final_climatology_tables, config["climatology_smoothing"]
).persist(StorageLevel.DISK_ONLY)

raw_monotonicity = monotonicity_summary(
    raw_test_scores, probability_col="p", tolerance=config["monotonicity_tolerance"]
)
calibrated_monotonicity = monotonicity_summary(
    calibrated_test_scores, probability_col="p", tolerance=config["monotonicity_tolerance"]
)
final_monotonicity = monotonicity_summary(
    final_test_scores, probability_col="p", tolerance=0.0
)
if final_monotonicity["strict_violation_rate"] != 0.0:
    raise AssertionError(
        f"PAVA output is not exactly non-increasing: {final_monotonicity}"
    )

test_model_metrics = []
for model_name, scores in (
    ("raw_selected", raw_test_scores),
    ("isotonic_calibrated", calibrated_test_scores),
    ("final_pava", final_test_scores),
    ("climatology", baseline_test_scores),
):
    test_model_metrics.append({
        "model": model_name,
        "per_threshold": threshold_metrics(scores, config["thresholds_mm"], config),
        "weighted_ece": weighted_ece(scores),
        "monotonicity": (
            raw_monotonicity if model_name == "raw_selected"
            else calibrated_monotonicity if model_name == "isotonic_calibrated"
            else final_monotonicity if model_name == "final_pava"
            else monotonicity_summary(scores, probability_col="p", tolerance=0.0)
        ),
    })

# Arbitrary in-domain threshold queries use linear interpolation on the
# projected anchor curve. Out-of-domain values are rejected by the helper.
unseen_thresholds = [float(value) for value in config["unseen_thresholds_mm"]]
domain_low, domain_high = map(float, config["threshold_domain_mm"])
if any(value < domain_low or value > domain_high for value in unseen_thresholds):
    raise ValueError("Configured interpolation thresholds exceed the frozen domain.")
unseen_queries = mark_unseen(
    expand_thresholds(test_base, config, unseen_thresholds), training_stations
)
unseen_scores = interpolate_thresholds(
    projected_test_scores,
    unseen_queries,
    config["thresholds_mm"],
    output_col="p",
    requested_thresholds=unseen_thresholds,
).persist(StorageLevel.DISK_ONLY)
unseen_metrics = threshold_metrics(unseen_scores, unseen_thresholds, config)
unseen_monotonicity = monotonicity_summary(
    unseen_scores, probability_col="p", tolerance=0.0
)
if unseen_monotonicity["strict_violation_rate"] != 0.0:
    raise AssertionError(
        f"Interpolated probabilities are not monotone: {unseen_monotonicity}"
    )

# Paired weekly-block bootstrap compares the final constrained predictor with
# full-training climatology on the same station/time/threshold cohort.
keys = ["station_id", "epoch", "threshold_mm"]
selected_losses = final_test_scores.select(
    *keys,
    "local_date",
    "label",
    F.pow(F.col("p") - F.col("label"), 2).alias("selected_loss"),
)
baseline_losses = baseline_test_scores.select(
    *keys,
    F.col("label").alias("baseline_label"),
    F.pow(F.col("p") - F.col("label"), 2).alias("baseline_loss"),
)
paired_losses = selected_losses.join(baseline_losses, keys, "inner").persist(
    StorageLevel.DISK_ONLY
)
paired_count = paired_losses.count()
if paired_losses.where("label != baseline_label").limit(1).count():
    raise ValueError("Paired test labels differ between predictor and climatology.")
if paired_count != final_test_scores.count() or paired_count != baseline_test_scores.count():
    raise ValueError("Final predictor and baseline test cohorts do not match.")

weekly = (
    paired_losses.withColumn("block", F.date_trunc("week", "local_date"))
    .groupBy("block")
    .agg(
        F.sum(F.col("selected_loss") - F.col("baseline_loss")).alias("difference_sum"),
        F.count("*").alias("n"),
    )
    .orderBy("block")
    .collect()
)
if len(weekly) > 60:
    raise ValueError("Expected at most 60 calendar-week blocks in the evaluation period.")
paired_losses.unpersist()
difference = sum(row["difference_sum"] for row in weekly) / sum(row["n"] for row in weekly)
if len(weekly) < 2:
    ci95 = None
else:
    rng = random.Random(config["sampling_seed"])
    bootstrap = []
    for _ in range(config["bootstrap_replicates"]):
        sample = [weekly[rng.randrange(len(weekly))] for _ in weekly]
        bootstrap.append(
            sum(row["difference_sum"] for row in sample)
            / sum(row["n"] for row in sample)
        )
    bootstrap.sort()
    ci95 = [
        bootstrap[int(0.025 * (len(bootstrap) - 1))],
        bootstrap[int(0.975 * (len(bootstrap) - 1))],
    ]
paired_weekly_brier = {
    "comparison": "final PAVA predictor minus climatology",
    "difference": difference,
    "ci95": ci95,
    "week_blocks": len(weekly),
    "bootstrap_replicates": config["bootstrap_replicates"] if ci95 else 0,
}

evaluation_summary = {
    "execution_recovery": {
        "mode": "resume_summary_and_interpolation_from_saved_holdout_predictions",
        "attempt": 3,
        "parent_notebook_04_state": (
            "failed_at_pava_projection; attempt_02_failed_at_unseen_interpolation"
        ),
        "reused_final_model_for_station_labels": True,
        "reused_calibrated_anchor_predictions": True,
        "model_scoring_recomputed": False,
        "calibrator_transform_recomputed": False,
        "recovery_module_sha256": recovery_module_sha256,
        "training_stations_source": "saved_pipeline_string_indexer_labels",
    },
    "protocol_version": 2,
    "selected_model": selected_name,
    "frozen_spec_sha256": frozen_spec_sha256,
    "heldout_evaluation_period": 2024,
    "historical_v1_2024_metrics_previously_observed": True,
    "historical_v1_metrics_used_for_v2_choices": False,
    "test_partition_opened_after_freeze": True,
    "post_test_tuning": False,
    "valid_test_base_rows": test_base_rows,
    "final_training_base_rows": final_training_base_rows,
    "final_training_expanded_rows": final_training_expanded_rows,
    "final_training_fraction": 1.0,
    "model_fit_seconds": model_fit_seconds,
    "calibration": calibration_summary,
    "metrics": test_model_metrics,
    "unseen_thresholds": {
        "method": "linear interpolation after PAVA",
        "domain_mm": [domain_low, domain_high],
        "thresholds_mm": unseen_thresholds,
        "metrics": unseen_metrics,
        "monotonicity": unseen_monotonicity,
    },
    "paired_weekly_brier": paired_weekly_brier,
    "final_monotonicity": final_monotonicity,
    "artifacts": {
        "selected_pipeline": (
            f"{MODEL_ROOT}/selected_pipeline" if selected_model is not None else None
        ),
        "climatology": climatology_root,
        "calibrator": f"{MODEL_ROOT}/isotonic_calibrator",
        "frozen_spec": f"{ROOT}/artifacts/frozen_spec.json",
        "anchor_scores": (
            f"{PARENT_RESULTS_ROOT}/scores/heldout_2024_anchor_predictions"
        ),
    },
}
summary_path = Path(tempfile.gettempdir()) / f"dsa5208-{RUN_ID}-final-summary.json"
summary_path.write_text(
    json.dumps(evaluation_summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
    encoding="utf-8",
)
gcloud([
    "storage", "cp", str(summary_path),
    f"{RESULTS_ROOT}/final_evaluation_summary.json",
])
summary_path.unlink(missing_ok=True)
display(evaluation_summary)

unseen_scores.unpersist()
final_test_scores.unpersist()
calibrated_test_scores.unpersist()
raw_test_scores.unpersist()
baseline_test_scores.unpersist()
test_anchor_queries.unpersist()
test_base.unpersist()
for frame in training_partitions:
    frame.unpersist()
training_stations.unpersist()
final_training_base.unpersist()
spark.stop()
