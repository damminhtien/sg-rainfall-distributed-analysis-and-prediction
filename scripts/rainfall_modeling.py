"""Shared Spark ML and probability utilities for the v2 notebooks."""

from __future__ import annotations

import math
from decimal import Decimal

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window, functions as F


BASE_FEATURES = ["hour_sin", "hour_cos", "day_sin", "day_cos", "threshold_mm"]
SCORE_COLUMNS = [
    "station_id", "epoch", "local_date", "month", "hour", "threshold_mm",
    "threshold_units", "label", "unseen_station", "onset",
]


def feature_columns(config, variant):
    if variant == "A":
        return list(BASE_FEATURES)
    if variant != "B":
        raise ValueError("Feature variant must be A or B.")

    columns = BASE_FEATURES + ["rain_current"]
    for minutes in config["lag_minutes"]:
        columns.extend([f"lag_{minutes}", f"lag_{minutes}_missing"])
    for minutes in config["rolling_minutes"]:
        columns.extend([
            f"sum_{minutes}", f"max_{minutes}", f"wet_count_{minutes}",
            f"count_{minutes}", f"coverage_{minutes}",
        ])
    return columns


def threshold_units(value, scale):
    scaled = Decimal(str(value)) * int(scale)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"Threshold {value} exceeds the stored numeric precision.")
    return int(scaled)


def expand_thresholds(frame, config, thresholds=None):
    values = list(thresholds if thresholds is not None else config["thresholds_mm"])
    rows = [
        (float(value), threshold_units(value, config["rainfall_integer_scale"]))
        for value in values
    ]
    grid = frame.sparkSession.createDataFrame(
        rows, "threshold_mm double, threshold_units long"
    )
    expanded = frame.crossJoin(F.broadcast(grid))
    if "future_units" in frame.columns:
        expanded = expanded.withColumn(
            "label", (F.col("future_units") > F.col("threshold_units")).cast("double")
        )
    return expanded


def deterministic_sample(frame, fraction, seed):
    fraction = float(fraction)
    if not 0 < fraction <= 1:
        raise ValueError("Sampling fraction must be in (0, 1].")
    if fraction == 1:
        return frame
    bucket = F.pmod(
        F.xxhash64("station_id", "epoch", F.lit(int(seed))), F.lit(1_000_000)
    )
    return frame.where(bucket < int(fraction * 1_000_000))


def mark_unseen(frame, seen_stations):
    return (
        frame.join(seen_stations.withColumn("_seen", F.lit(True)), "station_id", "left")
        .withColumn("unseen_station", F.col("_seen").isNull())
        .drop("_seen")
    )


def _preprocessing_stages(config, variant):
    numeric = feature_columns(config, variant)
    imputed = [f"{name}_imputed" for name in numeric]
    return [
        StringIndexer(inputCol="station_id", outputCol="station_index", handleInvalid="keep"),
        OneHotEncoder(
            inputCols=["station_index"], outputCols=["station_vector"],
            handleInvalid="keep", dropLast=False,
        ),
        Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=["station_vector", *imputed], outputCol="assembled"),
    ]


def build_pipeline(algorithm, parameters, variant, config):
    stages = _preprocessing_stages(config, variant)
    if algorithm == "lr":
        stages.extend([
            StandardScaler(
                inputCol="assembled", outputCol="features", withMean=False, withStd=True
            ),
            LogisticRegression(
                featuresCol="features", labelCol="label", maxIter=50,
                elasticNetParam=0.0, **parameters,
            ),
        ])
    elif algorithm == "rf":
        stages.append(RandomForestClassifier(
            featuresCol="assembled", labelCol="label", seed=int(config["sampling_seed"]),
            maxBins=64, **parameters,
        ))
    elif algorithm == "gbt":
        stages.append(GBTClassifier(
            featuresCol="assembled", labelCol="label", seed=int(config["sampling_seed"]),
            maxBins=64, **parameters,
        ))
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    return Pipeline(stages=stages)


def score_model(model, frame):
    transformed = model.transform(frame)
    metadata = [name for name in SCORE_COLUMNS if name in frame.columns]
    return transformed.select(
        *metadata, vector_to_array("probability")[1].alias("p")
    )


def _classification_summary(row, reference_cutoff):
    positives = int(row["positives"] or 0)
    tp, fp, fn = int(row["tp"] or 0), int(row["fp"] or 0), int(row["fn"] or 0)
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    row.update(
        positives=positives,
        prevalence=positives / row["n"] if row["n"] else None,
        reference_cutoff=float(reference_cutoff),
        precision=precision,
        recall=recall,
        f1=(
            2 * precision * recall / (precision + recall)
            if precision is not None and recall is not None and precision + recall
            else None
        ),
    )
    return row


def probability_metrics(scores, config, probability_col="p", reference_cutoff=0.5):
    p, y = F.col(probability_col), F.col("label")
    invalid = p.isNull() | F.isnan(p) | (p < 0) | (p > 1)
    row = scores.agg(
        F.count("*").alias("n"),
        F.sum(y).alias("positives"),
        F.avg(F.pow(p - y, 2)).alias("brier"),
        F.sum(invalid.cast("long")).alias("invalid_probabilities"),
        F.sum(((p >= reference_cutoff) & (y == 1)).cast("long")).alias("tp"),
        F.sum(((p >= reference_cutoff) & (y == 0)).cast("long")).alias("fp"),
        F.sum(((p < reference_cutoff) & (y == 1)).cast("long")).alias("fn"),
    ).first().asDict()

    if row["invalid_probabilities"]:
        raise ValueError(f"Found {row['invalid_probabilities']} invalid probabilities.")
    positives, n = int(row["positives"] or 0), int(row["n"] or 0)
    row["pr_auc"] = None
    if 0 < positives < n:
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol=probability_col, labelCol="label", metricName="areaUnderPR",
            numBins=int(config["auc_num_bins"]),
        )
        row["pr_auc"] = evaluator.evaluate(scores)
    return _classification_summary(row, reference_cutoff)


def threshold_metrics(scores, thresholds, config, probability_col="p"):
    return [
        {
            "threshold_mm": float(value),
            **probability_metrics(
                scores.where(F.col("threshold_mm") == float(value)), config,
                probability_col=probability_col,
            ),
        }
        for value in thresholds
    ]


def selection_objective(metrics, primary_thresholds):
    supported = [
        row for row in metrics
        if float(row["threshold_mm"]) in set(map(float, primary_thresholds))
        and row["pr_auc"] is not None
    ]
    if not supported:
        return None, math.inf
    return (
        sum(row["pr_auc"] for row in supported) / len(supported),
        sum(row["brier"] for row in supported) / len(supported),
    )


def choose_candidate(records, primary_thresholds, config):
    ranked = []
    for record in records:
        pr_auc, brier = selection_objective(record["metrics"], primary_thresholds)
        ranked.append((record, pr_auc, brier))
    eligible = [item for item in ranked if item[1] is not None]
    if not eligible:
        raise ValueError("No candidate has supported PR-AUC values.")

    best_pr_auc = max(item[1] for item in eligible)
    shortlist = [
        item for item in eligible
        if best_pr_auc - item[1] <= float(config["selection_pr_auc_tolerance"])
    ]
    best_brier = min(item[2] for item in shortlist)
    shortlist = [
        item for item in shortlist
        if item[2] - best_brier <= float(config["selection_brier_tolerance"])
    ]
    complexity = {"climatology": 0, "lr": 1, "rf": 2, "gbt": 3}
    return min(
        shortlist,
        key=lambda item: (
            complexity[item[0]["algorithm"]], item[0]["candidate_id"]
        ),
    )[0]


def fit_climatology_tables(train_base):
    def histogram(keys):
        return (
            train_base.groupBy(*keys, "future_units")
            .count()
            .withColumnRenamed("count", "frequency")
        )

    return {
        "station_hour": histogram(["station_id", "month", "hour"]),
        "month_hour": histogram(["month", "hour"]),
        "global": histogram([]),
    }


def climatology_scores(queries, tables, smoothing):
    keys = ["station_id", "month", "hour", "threshold_mm", "threshold_units"]
    query = queries.select(*keys).distinct()
    global_hist = tables["global"]
    month_hist = tables["month_hour"]
    station_hist = tables["station_hour"]

    global_p = (
        query.select("threshold_mm", "threshold_units").distinct()
        .crossJoin(global_hist)
        .groupBy("threshold_mm", "threshold_units")
        .agg(
            F.sum("frequency").alias("global_n"),
            F.sum(F.when(
                F.col("future_units") > F.col("threshold_units"), F.col("frequency")
            ).otherwise(0)).alias("global_positives"),
        )
        .withColumn("global_p", F.col("global_positives") / F.col("global_n"))
    )

    broad = (
        query.select("month", "hour", "threshold_mm", "threshold_units").distinct()
        .join(month_hist, ["month", "hour"], "left")
        .groupBy("month", "hour", "threshold_mm", "threshold_units")
        .agg(
            F.coalesce(F.sum("frequency"), F.lit(0)).alias("broad_n"),
            F.coalesce(F.sum(F.when(
                F.col("future_units") > F.col("threshold_units"), F.col("frequency")
            ).otherwise(0)), F.lit(0)).alias("broad_positives"),
        )
        .join(global_p.select("threshold_mm", "threshold_units", "global_p"),
              ["threshold_mm", "threshold_units"])
        .withColumn(
            "broad_p",
            (F.col("broad_positives") + float(smoothing) * F.col("global_p"))
            / (F.col("broad_n") + float(smoothing)),
        )
    )

    local = (
        query.join(station_hist, ["station_id", "month", "hour"], "left")
        .groupBy(*keys)
        .agg(
            F.coalesce(F.sum("frequency"), F.lit(0)).alias("local_n"),
            F.coalesce(F.sum(F.when(
                F.col("future_units") > F.col("threshold_units"), F.col("frequency")
            ).otherwise(0)), F.lit(0)).alias("local_positives"),
        )
        .join(broad.select(
            "month", "hour", "threshold_mm", "threshold_units", "broad_p"
        ), ["month", "hour", "threshold_mm", "threshold_units"])
        .withColumn(
            "p",
            (F.col("local_positives") + float(smoothing) * F.col("broad_p"))
            / (F.col("local_n") + float(smoothing)),
        )
    )
    metadata = [name for name in SCORE_COLUMNS if name in queries.columns]
    return queries.join(local.select(*keys, "p"), keys, "left").select(*metadata, "p")


def monotonicity_summary(scores, probability_col="p", tolerance=0.0):
    ordered = Window.partitionBy("station_id", "epoch").orderBy("threshold_mm")
    differences = (
        scores.withColumn("_previous_p", F.lag(probability_col).over(ordered))
        .where(F.col("_previous_p").isNotNull())
        .withColumn("_increase", F.col(probability_col) - F.col("_previous_p"))
    )
    row = differences.agg(
        F.count("*").alias("threshold_pairs"),
        F.avg((F.col("_increase") > float(tolerance)).cast("double")).alias("violation_rate"),
        F.avg((F.col("_increase") > 0.0).cast("double")).alias("strict_violation_rate"),
        F.max(F.greatest(F.col("_increase"), F.lit(0.0))).alias("max_increase"),
    ).first().asDict()
    return row


def project_nonincreasing(
    scores, thresholds, rainfall_integer_scale=1_000_000, probability_col="p"
):
    """Project each station-time probability vector onto the decreasing cone."""
    values = sorted(map(float, thresholds))
    if len(values) < 2 or len(set(values)) != len(values):
        raise ValueError("PAVA requires at least two unique threshold anchors.")
    units = [threshold_units(value, rainfall_integer_scale) for value in values]

    curve = F.sort_array(F.collect_list(F.struct(
        F.col("threshold_units").alias("units"), F.col(probability_col).alias("p")
    )))
    grouped = scores.groupBy("station_id", "epoch").agg(
        curve.alias("_curve"),
        F.count("*").alias("_n"),
        F.countDistinct("threshold_units").alias("_distinct_n"),
    )
    invalid = F.col("_n") != len(values)
    invalid = invalid | (F.col("_distinct_n") != len(values))
    for index, unit in enumerate(units):
        invalid = invalid | (
            F.col("_curve").getItem(index).getField("units") != int(unit)
        )
    if grouped.where(invalid).limit(1).count():
        raise ValueError("Each station-time must contain the complete ordered anchor grid.")

    for index in range(len(values)):
        candidate_means = []
        for end in range(index, len(values)):
            prefix_means = []
            for start in range(index + 1):
                segment = [
                    F.col("_curve").getItem(position).getField("p")
                    for position in range(start, end + 1)
                ]
                total = sum(segment, F.lit(0.0))
                prefix_means.append(total / float(len(segment)))
            candidate_means.append(
                prefix_means[0]
                if len(prefix_means) == 1
                else F.least(*prefix_means)
            )
        projected = (
            candidate_means[0]
            if len(candidate_means) == 1
            else F.greatest(*candidate_means)
        )
        if index:
            # Enforce exact floating-point feasibility after the closed-form PAVA.
            projected = F.least(projected, F.col(f"_projected_{index - 1}"))
        grouped = grouped.withColumn(f"_projected_{index}", projected)

    joined = scores.join(
        grouped.select(
            "station_id", "epoch",
            *[F.col(f"_projected_{i}") for i in range(len(values))],
        ),
        ["station_id", "epoch"], "inner",
    )
    output = F.lit(None).cast("double")
    for index, unit in enumerate(units):
        condition = F.col("threshold_units") == int(unit)
        output = F.when(condition, F.col(f"_projected_{index}")).otherwise(output)
    return joined.withColumn("projected_p", output)


def interpolate_thresholds(
    anchor_scores,
    requests,
    thresholds,
    output_col="p",
    requested_thresholds=None,
):
    """Linearly interpolate projected anchor probabilities inside the domain."""
    anchors = sorted(map(float, thresholds))
    if len(anchors) < 2 or len(set(anchors)) != len(anchors):
        raise ValueError("Interpolation requires at least two unique anchors.")
    if requested_thresholds is None:
        invalid = requests.where(
            (F.col("threshold_mm") < anchors[0])
            | (F.col("threshold_mm") > anchors[-1])
        ).limit(1).count()
    else:
        requested = list(map(float, requested_thresholds))
        invalid = any(value < anchors[0] or value > anchors[-1] for value in requested)
    if invalid:
        raise ValueError(
            f"Thresholds must remain inside the anchor domain [{anchors[0]}, {anchors[-1]}]."
        )

    curve = anchor_scores.groupBy("station_id", "epoch").agg(
        F.sort_array(F.collect_list(F.struct(
            F.col("threshold_mm").alias("threshold_mm"),
            F.col("projected_p").alias("p"),
        ))).alias("_curve")
    )
    joined = requests.join(curve, ["station_id", "epoch"], "inner")
    tau = F.col("threshold_mm")
    expression = F.col("_curve").getItem(len(anchors) - 1).getField("p")
    for index in reversed(range(len(anchors) - 1)):
        low, high = anchors[index], anchors[index + 1]
        p_low = F.col("_curve").getItem(index).getField("p")
        p_high = F.col("_curve").getItem(index + 1).getField("p")
        interpolated = p_low + (p_high - p_low) * (tau - low) / (high - low)
        expression = F.when(tau <= high, interpolated).otherwise(expression)
    return joined.withColumn(output_col, expression).drop("_curve")
