"""Spark probability metrics, calibration summaries and paired block uncertainty."""

import random

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import Window
from pyspark.sql import functions as F


def probability_metrics(scores, config, *, cutoff=0.5):
    invalid = (
        scores.where(
            F.col("p").isNull()
            | F.isnan("p")
            | ~F.col("p").between(0, 1)
            | ~F.col("label").isin(0.0, 1.0)
        )
        .limit(1)
        .count()
    )
    if invalid:
        raise ValueError("Scores contain invalid probabilities or labels.")
    epsilon = config["log_loss_epsilon"]
    p = F.greatest(F.lit(epsilon), F.least(F.lit(1 - epsilon), F.col("p")))
    row = (
        scores.agg(
            F.count("*").alias("n"),
            F.sum("label").alias("positives"),
            F.avg(F.pow(F.col("p") - F.col("label"), 2)).alias("brier"),
            F.avg(
                -F.col("label") * F.log(p) - (1 - F.col("label")) * F.log(1 - p)
            ).alias("log_loss"),
            F.sum(((F.col("p") >= cutoff) & (F.col("label") == 1)).cast("long")).alias(
                "tp"
            ),
            F.sum(((F.col("p") >= cutoff) & (F.col("label") == 0)).cast("long")).alias(
                "fp"
            ),
            F.sum(((F.col("p") < cutoff) & (F.col("label") == 1)).cast("long")).alias(
                "fn"
            ),
        )
        .first()
        .asDict()
    )
    n = row["n"]
    positives = int(row["positives"] or 0)
    row.update(
        positives=positives,
        prevalence=positives / n if n else None,
        cutoff=cutoff,
        pr_auc=None,
        roc_auc=None,
    )
    if 0 < positives < n:
        for metric, output in [("areaUnderPR", "pr_auc"), ("areaUnderROC", "roc_auc")]:
            evaluator = BinaryClassificationEvaluator(
                rawPredictionCol="p",
                labelCol="label",
                metricName=metric,
                numBins=config["auc_num_bins"],
            )
            row[output] = evaluator.evaluate(scores)
    precision = (
        row["tp"] / (row["tp"] + row["fp"]) if (row["tp"] or row["fp"]) else None
    )
    recall = row["tp"] / (row["tp"] + row["fn"]) if (row["tp"] or row["fn"]) else None
    row["precision"] = precision
    row["recall"] = recall
    row["f1"] = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )
    return row


def threshold_metrics(scores, config, *, cutoff=0.5):
    values = [
        r[0]
        for r in scores.select("threshold_mm")
        .distinct()
        .orderBy("threshold_mm")
        .collect()
    ]
    if len(values) > 32:
        raise ValueError("Unbounded threshold collection is not allowed.")
    return [
        dict(
            threshold_mm=value,
            **probability_metrics(
                scores.where(F.col("threshold_mm") == value), config, cutoff=cutoff
            ),
        )
        for value in values
    ]


def reliability(scores, config):
    count = config["calibration_bins"]
    return (
        scores.withColumn(
            "bin", F.least(F.floor(F.col("p") * count), F.lit(count - 1)).cast("int")
        )
        .groupBy("threshold_mm", "bin")
        .agg(
            F.avg("p").alias("predicted_probability"),
            F.avg("label").alias("observed_frequency"),
            F.count("*").alias("n"),
        )
    )


def pr_curve(scores, config):
    # Discrete frozen probability bins. Ties stay together, including the constant predictor.
    count = config["auc_num_bins"]
    grouped = (
        scores.withColumn("score_bin", F.floor(F.col("p") * count).cast("int"))
        .groupBy("threshold_mm", "score_bin")
        .agg(F.sum("label").alias("positives"), F.count("*").alias("n"))
    )
    ordered = (
        Window.partitionBy("threshold_mm")
        .orderBy(F.desc("score_bin"))
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    total = Window.partitionBy("threshold_mm")
    return (
        grouped.withColumn("tp", F.sum("positives").over(ordered))
        .withColumn("predicted_positive", F.sum("n").over(ordered))
        .withColumn("total_positives", F.sum("positives").over(total))
        .withColumn("precision", F.col("tp") / F.col("predicted_positive"))
        .withColumn(
            "recall",
            F.when(
                F.col("total_positives") > 0, F.col("tp") / F.col("total_positives")
            ),
        )
    )


def monotonicity(scores, config):
    order = Window.partitionBy("station_id", "epoch").orderBy("threshold_mm")
    comparison = (
        scores.withColumn("previous_p", F.lag("p").over(order))
        .where("previous_p IS NOT NULL")
        .withColumn("excess", F.col("p") - F.col("previous_p"))
    )
    return (
        comparison.agg(
            F.count("*").alias("comparisons"),
            F.avg(
                (F.col("excess") > config["monotonicity_tolerance"]).cast("double")
            ).alias("violation_rate"),
            F.max(F.greatest(F.col("excess"), F.lit(0))).alias("max_excess"),
        )
        .first()
        .asDict()
    )


def error_strata(scores):
    outputs = {}
    for columns in [
        ("month", "threshold_mm"),
        ("station_id", "threshold_mm"),
        ("onset", "threshold_mm"),
        ("unseen_station", "threshold_mm"),
    ]:
        if all(name in scores.columns for name in columns):
            outputs["_".join(columns)] = scores.groupBy(*columns).agg(
                F.count("*").alias("n"),
                F.avg("label").alias("prevalence"),
                F.avg(F.pow(F.col("p") - F.col("label"), 2)).alias("brier"),
                F.avg("p").alias("mean_p"),
            )
    return outputs


def paired_weekly_brier(scores_a, scores_b, config):
    keys = ["station_id", "epoch", "threshold_mm"]
    a = scores_a.select(
        *keys,
        "local_date",
        "label",
        F.pow(F.col("p") - F.col("label"), 2).alias("loss_a"),
    )
    b = scores_b.select(
        *keys,
        F.col("label").alias("label_b"),
        F.pow(F.col("p") - F.col("label"), 2).alias("loss_b"),
    )
    paired = a.join(b, keys, "inner")
    if paired.where("label != label_b").limit(1).count():
        raise ValueError("Paired labels differ.")
    expected_a = a.count()
    if paired.count() != expected_a or b.count() != expected_a:
        raise ValueError("Paired cohorts must match exactly.")
    weeks = (
        paired.withColumn("block", F.date_trunc("week", "local_date"))
        .groupBy("block")
        .agg(
            F.sum(F.col("loss_b") - F.col("loss_a")).alias("difference_sum"),
            F.count("*").alias("n"),
        )
        .orderBy("block")
        .collect()
    )
    if len(weeks) > 60:
        raise ValueError("Expected at most 60 weekly blocks for one final-test year.")
    if len(weeks) < 2:
        return {"blocks": len(weeks), "difference": None, "ci95": None}
    rng = random.Random(config["sampling_seed"])
    values = []
    for _ in range(config["bootstrap_replicates"]):
        sampled = [weeks[rng.randrange(len(weeks))] for _ in weeks]
        values.append(
            sum(r["difference_sum"] for r in sampled) / sum(r["n"] for r in sampled)
        )
    values.sort()
    low = values[int(0.025 * (len(values) - 1))]
    high = values[int(0.975 * (len(values) - 1))]
    return {
        "blocks": len(weeks),
        "difference_b_minus_a": sum(r["difference_sum"] for r in weeks)
        / sum(r["n"] for r in weeks),
        "ci95": [low, high],
        "replicates": len(values),
        "block_unit": "calendar week, all stations/thresholds jointly",
    }
