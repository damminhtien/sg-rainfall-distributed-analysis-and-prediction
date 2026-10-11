"""Elapsed-time history features and exact contiguous 30-minute targets."""

from datetime import datetime
from decimal import Decimal

from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import require_label_semantics

TEMPORAL_FEATURES = ["hour_sin", "hour_cos", "day_sin", "day_cos", "threshold_mm"]


def threshold_units(value, scale):
    scaled = Decimal(str(value)) * scale
    if scaled != scaled.to_integral_value():
        raise ValueError("Threshold exceeds supported numerical precision.")
    return int(scaled)


def feature_columns(config, variant="B"):
    columns = list(TEMPORAL_FEATURES)
    if variant == "B":
        columns += ["rain_current", "current_missing", "latest_reading_age_minutes"]
        for minutes in config["lag_minutes"]:
            columns += [f"lag_{minutes}", f"lag_{minutes}_missing"]
        for minutes in config["rolling_minutes"]:
            columns += [
                f"sum_{minutes}",
                f"max_{minutes}",
                f"wet_count_{minutes}",
                f"count_{minutes}",
                f"coverage_{minutes}",
            ]
    elif variant != "A":
        raise ValueError("Only A and B are implemented.")
    return columns


def build_base_features(clean, config, *, with_labels=True):
    """One row per observed station/time; missing current times are excluded from training.

    For inference, append a query row with null rain_units before calling with_labels=False.
    All windows operate on unique exact-grid epoch keys. Final archived values imply
    retrospective event-time evaluation; update timestamps are not publication proof.
    """
    if with_labels:
        require_label_semantics(config)
    key = Window.partitionBy("station_id").orderBy("epoch")
    frame = clean.withColumn("rain_current", F.col("rain_mm")).withColumn(
        "current_missing", F.col("rain_mm").isNull().cast("double")
    )
    for minutes in config["lag_minutes"]:
        lag = F.first("rain_mm", ignorenulls=True).over(
            key.rangeBetween(-minutes * 60, -minutes * 60)
        )
        frame = frame.withColumn(f"lag_{minutes}", lag).withColumn(
            f"lag_{minutes}_missing", F.col(f"lag_{minutes}").isNull().cast("double")
        )
    for minutes in config["rolling_minutes"]:
        window = key.rangeBetween(-(minutes - 5) * 60, 0)
        frame = frame.withColumn(f"sum_{minutes}", F.sum("rain_mm").over(window))
        frame = frame.withColumn(f"max_{minutes}", F.max("rain_mm").over(window))
        frame = frame.withColumn(
            f"wet_count_{minutes}",
            F.sum(F.when(F.col("rain_mm") > 0, 1).otherwise(0)).over(window),
        )
        frame = frame.withColumn(
            f"count_{minutes}", F.count("rain_mm").over(window).cast("double")
        )
        frame = frame.withColumn(
            f"coverage_{minutes}", F.col(f"count_{minutes}") / (minutes / 5)
        )
    last_seen = F.max(F.when(F.col("rain_mm").isNotNull(), F.col("epoch"))).over(
        key.rangeBetween(-10800, 0)
    )
    frame = frame.withColumn(
        "latest_reading_age_minutes", (F.col("epoch") - last_seen) / 60.0
    )
    time = F.to_timestamp(F.from_unixtime("epoch"))
    fractional_hour = F.hour(time) + F.minute(time) / 60.0
    year_start = F.make_date(F.year(time), F.lit(1), F.lit(1))
    next_year = F.make_date(F.year(time) + 1, F.lit(1), F.lit(1))
    day_fraction = (F.dayofyear(time) - 1 + fractional_hour / 24.0) / F.datediff(
        next_year, year_start
    )
    frame = frame.withColumn(
        "hour_sin", F.sin(2 * F.lit(3.141592653589793) * fractional_hour / 24)
    )
    frame = frame.withColumn(
        "hour_cos", F.cos(2 * F.lit(3.141592653589793) * fractional_hour / 24)
    )
    frame = frame.withColumn(
        "day_sin", F.sin(2 * F.lit(3.141592653589793) * day_fraction)
    )
    frame = frame.withColumn(
        "day_cos", F.cos(2 * F.lit(3.141592653589793) * day_fraction)
    )
    frame = (
        frame.withColumn("month", F.month(time))
        .withColumn("hour", F.hour(time))
        .withColumn("local_date", F.to_date(time))
    )
    frame = frame.withColumn("onset", F.coalesce(F.col("sum_30"), F.lit(0)) == 0)
    if with_labels:
        future = key.rangeBetween(300, 1800)
        frame = frame.withColumn("future_units", F.sum("rain_units").over(future))
        frame = frame.withColumn("future_count", F.count("rain_units").over(future))
        frame = frame.withColumn(
            "future_min_epoch",
            F.min(F.when(F.col("rain_units").isNotNull(), F.col("epoch"))).over(future),
        )
        frame = frame.withColumn(
            "future_max_epoch",
            F.max(F.when(F.col("rain_units").isNotNull(), F.col("epoch"))).over(future),
        )
        frame = frame.withColumn(
            "label_valid",
            (F.col("future_count") == 6)
            & (F.col("future_min_epoch") == F.col("epoch") + 300)
            & (F.col("future_max_epoch") == F.col("epoch") + 1800),
        )
    return frame


def assign_splits(base, config):
    bounds = [
        int(datetime.fromisoformat(value).timestamp())
        for value in config["split_boundaries"]
    ]
    expression = F.lit(None).cast("string")
    for name, start, end in zip(config["split_names"], bounds[:-1], bounds[1:]):
        expression = F.when(
            (F.col("epoch") >= start) & (F.col("epoch") + 1800 < end), F.lit(name)
        ).otherwise(expression)
    return base.withColumn("split", expression)


def expand_thresholds(base, config, *, thresholds=None):
    values = thresholds if thresholds is not None else config["thresholds_mm"]
    rows = [
        (float(value), threshold_units(value, config["rainfall_integer_scale"]))
        for value in values
    ]
    grid = base.sparkSession.createDataFrame(
        rows, "threshold_mm double, threshold_units long"
    )
    expanded = base.crossJoin(F.broadcast(grid))
    if "future_units" in base.columns:
        expanded = expanded.withColumn(
            "label", (F.col("future_units") > F.col("threshold_units")).cast("double")
        )
    return expanded


def deterministic_sample(base, fraction, seed):
    """Sample base station/time keys before threshold expansion; prevalence is untouched."""
    if fraction == 1:
        return base
    bucket = F.pmod(F.xxhash64("station_id", "epoch", F.lit(seed)), F.lit(1000000))
    return base.where(bucket < int(fraction * 1000000))


def feature_dictionary(config):
    rows = [
        {
            "name": name,
            "source": "prediction time or query threshold",
            "availability": config["availability_mode"],
            "missing_rule": "none",
        }
        for name in TEMPORAL_FEATURES
    ]
    for name in feature_columns(config, "B"):
        if name not in TEMPORAL_FEATURES:
            rows.append(
                {
                    "name": name,
                    "source": "same-station exact elapsed-time history at or before t",
                    "availability": config["availability_mode"],
                    "missing_rule": "train-fitted median imputation plus indicators/counts",
                }
            )
    return rows
