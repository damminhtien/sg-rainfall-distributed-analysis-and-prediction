"""Bounded temporal tuning, finalist fitting, calibration and frozen evaluation."""

import json
import time
from datetime import UTC, datetime

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from .config import config_hash
from .features import deterministic_sample, expand_thresholds
from .metrics import (
    error_strata,
    monotonicity,
    paired_weekly_brier,
    pr_curve,
    reliability,
    threshold_metrics,
)
from .models import (
    Climatology,
    calibrated_scores,
    fit_calibrator,
    pipeline,
    score,
    select_candidate,
)


def compact_scores(frame):
    columns = [
        "station_id",
        "epoch",
        "threshold_mm",
        "threshold_units",
        "label",
        "local_date",
        "month",
        "hour",
        "onset",
        "unseen_station",
        "p",
    ]
    return frame.select(*[name for name in columns if name in frame.columns])


def mark_unseen(frame, stations):
    return (
        frame.join(stations.withColumn("_seen", F.lit(True)), "station_id", "left")
        .withColumn("unseen_station", F.col("_seen").isNull())
        .drop("_seen")
    )


def write_record(spark, path, record):
    # A single JSON string avoids ambiguous inferred types in nested metric records.
    spark.createDataFrame(
        [(json.dumps(record, allow_nan=False),)], "value string"
    ).coalesce(1).write.mode("errorifexists").text(path)


def read_record(spark, path):
    rows = spark.read.text(path).limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Expected one metadata record.")
    return json.loads(rows[0]["value"])


def fit_models(base, config, output_root, *, manifest_sha256, git_commit):
    train_base = base.where("split = 'train' AND label_valid")
    selection_base = base.where("split = 'selection' AND label_valid")
    calibration_base = base.where("split = 'calibration' AND label_valid")
    if (
        min(
            train_base.limit(1).count(),
            selection_base.limit(1).count(),
            calibration_base.limit(1).count(),
        )
        == 0
    ):
        raise ValueError("Each temporal subset must contain valid samples.")
    support = (
        expand_thresholds(train_base, config)
        .groupBy("threshold_mm")
        .agg(F.count("*").alias("n"), F.sum("label").alias("positives"))
        .collect()
    )
    primary = [
        r["threshold_mm"]
        for r in support
        if config["primary_threshold_min_positives"] <= r["positives"] < r["n"]
    ]
    if not primary:
        raise ValueError("Training support does not justify any primary threshold.")
    stations = train_base.select("station_id").distinct().persist()
    stations.write.mode("errorifexists").parquet(f"{output_root}/training_stations")
    pilot = expand_thresholds(
        deterministic_sample(
            train_base, config["pilot_fraction"], config["sampling_seed"]
        ),
        config,
    ).persist(StorageLevel.DISK_ONLY)
    train = expand_thresholds(
        deterministic_sample(
            train_base, config["final_train_fraction"], config["sampling_seed"]
        ),
        config,
    ).persist(StorageLevel.DISK_ONLY)
    selection = mark_unseen(
        expand_thresholds(selection_base, config), stations
    ).persist(StorageLevel.DISK_ONLY)
    n_pilot, n_train = pilot.count(), train.count()
    n_selection = selection.count()
    candidates = []
    finalists = {}
    records = []
    try:
        baseline = Climatology.fit(train_base, config)
        baseline.save(f"{output_root}/climatology")
        baseline_scores = compact_scores(baseline.transform(selection)).persist(
            StorageLevel.DISK_ONLY
        )
        baseline_record = {
            "candidate_id": "climatology",
            "algorithm": "climatology",
            "variant": "A",
            "metrics": threshold_metrics(baseline_scores, config),
            "path": f"{output_root}/climatology",
        }
        records.append(baseline_record)
        baseline_scores.unpersist()
        for variant in ("A", "B"):
            for algorithm in ("lr", "gbt"):
                options = (
                    [{"regParam": value} for value in config["lr_reg_params"]]
                    if algorithm == "lr"
                    else config["gbt_configs"]
                )
                local_candidates = []
                for index, parameters in enumerate(options):
                    candidate_id = f"{algorithm}_{variant}_{index}"
                    started = time.monotonic()
                    fitted = pipeline(
                        config, algorithm, parameters, variant=variant
                    ).fit(pilot)
                    predictions = compact_scores(score(fitted, selection)).persist(
                        StorageLevel.DISK_ONLY
                    )
                    record = {
                        "candidate_id": candidate_id,
                        "algorithm": algorithm,
                        "variant": variant,
                        "parameters": parameters,
                        "fit_seconds": time.monotonic() - started,
                        "metrics": threshold_metrics(predictions, config),
                    }
                    candidates.append(record)
                    local_candidates.append(record)
                    print(
                        f"Completed pilot candidate {candidate_id}: {record['fit_seconds']:.1f}s"
                    )
                    predictions.unpersist()
                chosen = select_candidate(local_candidates, config, primary)
                started = time.monotonic()
                fitted = pipeline(
                    config, algorithm, chosen["parameters"], variant=variant
                ).fit(train)
                name = f"{algorithm}_{variant}"
                path = f"{output_root}/{name}"
                fitted.write().save(path)
                finalists[name] = fitted
                predictions = compact_scores(score(fitted, selection)).persist(
                    StorageLevel.DISK_ONLY
                )
                record = {
                    "candidate_id": name,
                    "algorithm": algorithm,
                    "variant": variant,
                    "parameters": chosen["parameters"],
                    "fit_seconds": time.monotonic() - started,
                    "metrics": threshold_metrics(predictions, config),
                    "path": path,
                }
                records.append(record)
                predictions.unpersist()
        selected = select_candidate(
            [
                r
                for r in records
                if r["candidate_id"] == "climatology" or r["variant"] == "B"
            ],
            config,
            primary,
        )
        model = (
            baseline
            if selected["algorithm"] == "climatology"
            else finalists[selected["candidate_id"]]
        )
        selection_scores = compact_scores(
            model.transform(selection)
            if selected["algorithm"] == "climatology"
            else score(model, selection)
        )
        calibration_summary = reliability(selection_scores, config)
        ece = calibration_summary.agg(
            (
                F.sum(
                    F.abs(F.col("predicted_probability") - F.col("observed_frequency"))
                    * F.col("n")
                )
                / F.sum("n")
            ).alias("ece")
        ).first()["ece"]
        use_calibration = ece > config["calibration_ece_trigger"]
        cutoffs = []
        for cutoff in config["probability_cutoffs"]:
            values = threshold_metrics(selection_scores, config, cutoff=cutoff)
            f1 = [
                r["f1"]
                for r in values
                if r["threshold_mm"] in primary and r["f1"] is not None
            ]
            cutoffs.append((sum(f1) / len(f1) if f1 else -1.0, cutoff))
        cutoff = max(cutoffs, key=lambda item: (item[0], -item[1]))[1]
        if use_calibration:
            calibration = mark_unseen(
                expand_thresholds(calibration_base, config), stations
            )
            calibration_scores = compact_scores(
                model.transform(calibration)
                if selected["algorithm"] == "climatology"
                else score(model, calibration)
            )
            calibrator = fit_calibrator(calibration_scores)
            calibrator.write().save(f"{output_root}/calibrator")
        record = {
            "created_at_utc": datetime.now(UTC).isoformat(),
            "config": config,
            "config_sha256": config_hash(config),
            "manifest_sha256": manifest_sha256,
            "git_commit": git_commit,
            "primary_thresholds": sorted(primary),
            "training_support": [r.asDict() for r in support],
            "pilot_expanded_n": n_pilot,
            "final_train_expanded_n": n_train,
            "selection_expanded_n": n_selection,
            "training_scope": f"MLlib finalists: deterministic {config['final_train_fraction']:.1%} of 2017–2022 base station/time keys; climatology: all training base samples",
            "pilot_candidates": candidates,
            "finalists": records,
            "selected": selected,
            "algorithm": selected["algorithm"],
            "use_calibration": use_calibration,
            "selection_ece": ece,
            "probability_cutoff": cutoff,
            "calibrator_recipe": "pooled monotone isotonic mapping on Jul–Dec 2023 if selection ECE exceeds frozen trigger",
            "model_root": output_root,
            "test_accessed": False,
        }
        write_record(base.sparkSession, f"{output_root}/frozen_manifest", record)
        return record
    finally:
        pilot.unpersist()
        train.unpersist()
        selection.unpersist()
        stations.unpersist()


def final_evaluation(base, frozen, output_root):
    config = frozen["config"]
    if frozen["config_sha256"] != config_hash(config) or frozen["test_accessed"]:
        raise ValueError("Invalid or already-accessed freeze record.")
    spark = base.sparkSession
    model_root = frozen["model_root"]
    stations = spark.read.parquet(f"{model_root}/training_stations")
    test_base = base.where("split = 'test' AND label_valid")
    test = mark_unseen(expand_thresholds(test_base, config), stations).persist(
        StorageLevel.DISK_ONLY
    )
    count = test.count()
    if count == 0:
        raise ValueError("Final test has no valid rows.")
    selected = frozen["selected"]["candidate_id"]
    models = {"climatology": Climatology.load(spark, f"{model_root}/climatology")}
    for record in frozen["finalists"]:
        if record["algorithm"] != "climatology":
            models[record["candidate_id"]] = PipelineModel.load(record["path"])
    from pyspark.ml.regression import IsotonicRegressionModel

    calibrator = (
        IsotonicRegressionModel.load(f"{model_root}/calibrator")
        if frozen["use_calibration"]
        else None
    )
    summaries = []
    try:
        for name, model in models.items():
            predictions = compact_scores(
                model.transform(test) if name == "climatology" else score(model, test)
            ).persist(StorageLevel.DISK_ONLY)
            predictions.write.mode("errorifexists").parquet(
                f"{output_root}/scores/{name}"
            )
            metrics = threshold_metrics(
                predictions, config, cutoff=frozen["probability_cutoff"]
            )
            summary = {
                "model": name,
                "per_threshold": metrics,
                "monotonicity": monotonicity(predictions, config),
            }
            summaries.append(summary)
            reliability(predictions, config).write.mode("errorifexists").json(
                f"{output_root}/reliability/{name}"
            )
            pr_curve(
                predictions.where(
                    F.col("threshold_mm").isin(frozen["primary_thresholds"][:3])
                ),
                config,
            ).write.mode("errorifexists").json(f"{output_root}/pr_curves/{name}")
            for stratum, frame in error_strata(predictions).items():
                frame.write.mode("errorifexists").json(
                    f"{output_root}/errors/{name}/{stratum}"
                )
            if name == selected and calibrator is not None:
                corrected = compact_scores(
                    calibrated_scores(predictions, calibrator)
                ).persist(StorageLevel.DISK_ONLY)
                corrected.write.mode("errorifexists").parquet(
                    f"{output_root}/scores/selected_calibrated"
                )
                summaries.append(
                    {
                        "model": "selected_calibrated",
                        "per_threshold": threshold_metrics(
                            corrected, config, cutoff=frozen["probability_cutoff"]
                        ),
                        "monotonicity": monotonicity(corrected, config),
                    }
                )
                reliability(corrected, config).write.mode("errorifexists").json(
                    f"{output_root}/reliability/selected_calibrated"
                )
                corrected.unpersist()
            predictions.unpersist()
        baseline_scores = spark.read.parquet(f"{output_root}/scores/climatology")
        selected_name = "selected_calibrated" if calibrator is not None else selected
        selected_scores = spark.read.parquet(f"{output_root}/scores/{selected_name}")
        uncertainty = paired_weekly_brier(baseline_scores, selected_scores, config)
        unseen = mark_unseen(
            expand_thresholds(
                test_base, config, thresholds=config["unseen_thresholds_mm"]
            ),
            stations,
        )
        chosen_model = models[selected]
        unseen_scores = compact_scores(
            chosen_model.transform(unseen)
            if selected == "climatology"
            else score(chosen_model, unseen)
        )
        if calibrator is not None:
            unseen_scores = compact_scores(calibrated_scores(unseen_scores, calibrator))
        unseen_metrics = threshold_metrics(
            unseen_scores, config, cutoff=frozen["probability_cutoff"]
        )
        result = {
            "created_at_utc": datetime.now(UTC).isoformat(),
            "test_expanded_n": count,
            "models": summaries,
            "selected": selected,
            "selected_scoring": selected_name,
            "unseen_threshold_metrics": unseen_metrics,
            "unseen_monotonicity": monotonicity(unseen_scores, config),
            "paired_week_brier": uncertainty,
            "config_sha256": frozen["config_sha256"],
            "post_test_tuning": False,
        }
        write_record(spark, f"{output_root}/evaluation_manifest", result)
        return result
    finally:
        test.unpersist()
