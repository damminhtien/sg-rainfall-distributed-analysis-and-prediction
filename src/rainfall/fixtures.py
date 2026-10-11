"""Small meaningful checks executed in the Dataproc Jupyter kernel."""

from copy import deepcopy
from datetime import datetime

from pyspark.sql import functions as F

from .features import (
    assign_splits,
    build_base_features,
    expand_thresholds,
    feature_columns,
)


def run_fixtures(spark, config):
    # A synthetic contract is explicit; this does not establish real NEA semantics.
    synthetic = deepcopy(config)
    synthetic["interval_semantics_confirmed"] = True
    start = int(datetime.fromisoformat("2022-12-31T23:00:00+08:00").timestamp())
    rows = [
        (
            "S1",
            start + i * 300,
            float(i % 3),
            int(i % 3) * config["rainfall_integer_scale"],
        )
        for i in range(25)
    ]
    frame = spark.createDataFrame(
        rows, "station_id string, epoch long, rain_mm double, rain_units long"
    )
    base = build_base_features(frame, synthetic)
    t = start + 3600
    one = base.where(F.col("epoch") == t).first()
    expected = sum(rows[i][3] for i in range(13, 19))
    assert one["future_units"] == expected and one["label_valid"]
    threshold = expected / config["rainfall_integer_scale"]
    equal = expand_thresholds(
        base.where(F.col("epoch") == t), synthetic, thresholds=[threshold]
    ).first()
    assert equal["label"] == 0, "Equality must be a negative label."
    # Missing interior interval must invalidate the horizon rather than shift six rows.
    gap = frame.where(F.col("epoch") != t + 900)
    assert (
        not build_base_features(gap, synthetic)
        .where(F.col("epoch") == t)
        .first()["label_valid"]
    )
    # Future rainfall mutation cannot alter a single history feature at t.
    changed = frame.withColumn(
        "rain_mm", F.when(F.col("epoch") > t, 999.0).otherwise(F.col("rain_mm"))
    ).withColumn(
        "rain_units",
        F.when(F.col("epoch") > t, 999 * config["rainfall_integer_scale"]).otherwise(
            F.col("rain_units")
        ),
    )
    before = (
        build_base_features(frame, synthetic, with_labels=False)
        .where(F.col("epoch") == t)
        .first()
        .asDict()
    )
    after = (
        build_base_features(changed, synthetic, with_labels=False)
        .where(F.col("epoch") == t)
        .first()
        .asDict()
    )
    for name in feature_columns(synthetic, "B"):
        if name != "threshold_mm":
            assert before[name] == after[name], f"Future leakage: {name}"
    # Dec 31 23:30 endpoint is Jan 1 00:00 and must be purged from train.
    boundary = start + 1800
    split = assign_splits(base.where(F.col("epoch") == boundary), synthetic).first()[
        "split"
    ]
    assert split is None, "Labels must not cross temporal split boundaries."
    # Dry case and station separation.
    dry = frame.withColumn("rain_mm", F.lit(0.0)).withColumn(
        "rain_units", F.lit(0).cast("long")
    )
    assert (
        expand_thresholds(
            build_base_features(dry, synthetic).where(F.col("epoch") == t),
            synthetic,
            thresholds=[0.2],
        ).first()["label"]
        == 0
    )
    other = (
        frame.withColumn("station_id", F.lit("S2"))
        .withColumn("rain_mm", F.lit(500.0))
        .withColumn("rain_units", F.lit(500 * config["rainfall_integer_scale"]))
    )
    mixed = (
        build_base_features(frame.unionByName(other), synthetic)
        .where((F.col("epoch") == t) & (F.col("station_id") == "S1"))
        .first()
    )
    assert mixed["future_units"] == expected
    return {
        "fixtures": [
            "exact_horizon",
            "strict_equality",
            "interior_gap",
            "future_feature_leakage",
            "split_boundary",
            "dry_case",
            "station_isolation",
        ],
        "status": "passed",
    }
