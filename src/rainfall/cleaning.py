"""Deterministic observation cleaning with disjoint exclusion reasons."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from scripts.run_quality_audit import (
    CORRUPT_RECORD_COLUMN,
    EXPECTED_SCHEMA,
    add_parsed_columns,
    is_finite,
)


def flag_reason(frame, condition, reason):
    return frame.withColumn(
        "exclusion_reason",
        F.when(F.col("exclusion_reason").isNotNull(), F.col("exclusion_reason")).when(
            F.coalesce(condition, F.lit(True)), F.lit(reason)
        ),
    )


def clean_observations(raw, config):
    """Keep exact-grid records; no timestamp rounding or rainfall interpolation."""
    parsed = add_parsed_columns(F, raw).withColumn(
        "exclusion_reason", F.lit(None).cast("string")
    )
    rules = [
        (F.col(CORRUPT_RECORD_COLUMN).isNotNull(), "csv_parser_error"),
        (
            F.col("station_id").isNull() | (F.trim("station_id") == ""),
            "missing_station",
        ),
        (F.col("_timestamp_parsed").isNull(), "invalid_timestamp"),
        (
            F.col("_source_date_parsed").isNull()
            | (F.col("_source_date_parsed") != F.to_date("_timestamp_parsed")),
            "date_mismatch",
        ),
        (F.year("_timestamp_parsed") != F.col("_source_year"), "year_mismatch"),
        (~is_finite(F, F.col("_reading_value_numeric")), "invalid_rainfall"),
        (F.col("_reading_value_numeric") < 0, "negative_rainfall"),
        (F.trim("reading_unit") != "mm", "invalid_unit"),
        (F.trim("reading_type") != config["reading_type"], "invalid_reading_type"),
        (
            ~is_finite(F, F.col("_location_longitude_numeric"))
            | ~is_finite(F, F.col("_location_latitude_numeric")),
            "invalid_coordinates",
        ),
        (
            ~F.col("_location_longitude_numeric").between(-180, 180)
            | ~F.col("_location_latitude_numeric").between(-90, 90),
            "coordinates_out_of_range",
        ),
    ]
    for condition, reason in rules:
        parsed = flag_reason(parsed, condition, reason)
    parsed = parsed.withColumn("epoch", F.col("_timestamp_parsed").cast("long"))
    parsed = parsed.withColumn("alignment_offset_seconds", F.pmod("epoch", F.lit(300)))
    alignment = parsed.groupBy("_source_year", "alignment_offset_seconds").count()
    parsed = flag_reason(
        parsed, F.col("alignment_offset_seconds") != 0, "off_grid_timestamp"
    )
    parsed = parsed.withColumn(
        "rain_units",
        F.round(
            F.col("_reading_value_numeric") * config["rainfall_integer_scale"]
        ).cast("long"),
    )
    parsed = flag_reason(
        parsed,
        F.abs(
            F.col("rain_units") / config["rainfall_integer_scale"]
            - F.col("_reading_value_numeric")
        )
        > 1e-10,
        "unsupported_rainfall_precision",
    )
    base_rejected = parsed.where("exclusion_reason IS NOT NULL")
    candidates = parsed.where("exclusion_reason IS NULL").withColumn(
        "station_id", F.trim("station_id")
    )
    # Order exactly equal raw rows by provenance; their measurements are identical.
    exact_order = Window.partitionBy(*EXPECTED_SCHEMA, "_source_year").orderBy(
        "_source_file"
    )
    candidates = candidates.withColumn("_exact_rank", F.row_number().over(exact_order))
    exact_rejected = candidates.where("_exact_rank > 1").withColumn(
        "exclusion_reason", F.lit("exact_duplicate")
    )
    unique = candidates.where("_exact_rank = 1")
    key_window = Window.partitionBy("station_id", "epoch")
    # Include coordinates/device/name to avoid silently choosing conflicting metadata.
    variants = unique.groupBy("station_id", "epoch").agg(
        F.countDistinct(
            F.struct(
                "rain_units",
                "station_name",
                "station_device_id",
                "_location_longitude_numeric",
                "_location_latitude_numeric",
            )
        ).alias("_variants")
    )
    unique = unique.join(variants, ["station_id", "epoch"])
    key_order = key_window.orderBy(
        "_update_timestamp_parsed", "_reading_update_timestamp_parsed", "_source_file"
    )
    unique = unique.withColumn("_key_rank", F.row_number().over(key_order))
    conflicts = unique.where("_variants > 1").withColumn(
        "exclusion_reason", F.lit("conflicting_key")
    )
    equal_repeats = unique.where("_variants = 1 AND _key_rank > 1").withColumn(
        "exclusion_reason", F.lit("same_measurement_duplicate")
    )
    rejected = (
        base_rejected.unionByName(exact_rejected, allowMissingColumns=True)
        .unionByName(conflicts, allowMissingColumns=True)
        .unionByName(equal_repeats, allowMissingColumns=True)
    )
    retained = unique.where("_variants = 1 AND _key_rank = 1")
    clean = retained.select(
        "station_id",
        "epoch",
        "rain_units",
        F.col("_timestamp_parsed").alias("observation_time"),
        F.col("_reading_value_numeric").alias("rain_mm"),
        F.col("_location_longitude_numeric").alias("longitude"),
        F.col("_location_latitude_numeric").alias("latitude"),
        F.col("_source_year").alias("year"),
        F.to_date("_timestamp_parsed").alias("local_date"),
        F.month("_timestamp_parsed").alias("month"),
        F.hour("_timestamp_parsed").alias("hour"),
        F.col("_update_timestamp_parsed").alias("update_time"),
        F.col("_reading_update_timestamp_parsed").alias("reading_update_time"),
        "station_name",
        "station_device_id",
        "_source_file",
        "alignment_offset_seconds",
    )
    exclusions = rejected.groupBy("_source_year", "exclusion_reason").count()
    quality = (
        raw.groupBy("_source_year")
        .count()
        .withColumnRenamed("count", "raw_rows")
        .join(
            clean.groupBy("year").agg(
                F.count("*").alias("retained_rows"),
                F.countDistinct("station_id").alias("stations"),
            ),
            F.col("_source_year") == F.col("year"),
            "left",
        )
        .drop("year")
        .withColumnRenamed("_source_year", "year")
        .fillna(0)
    )
    return clean, rejected, quality, exclusions, alignment


def coverage(clean):
    """Use full calendar periods as denominators, including leap days."""
    months = clean.groupBy("station_id", "year", "month").agg(
        F.count("*").alias("valid_slots"),
        F.countDistinct("local_date").alias("observed_days"),
        F.min("observation_time").alias("first_observation"),
        F.max("observation_time").alias("last_observation"),
    )
    month_start = F.make_date("year", "month", F.lit(1))
    months = months.withColumn(
        "expected_slots", F.dayofmonth(F.last_day(month_start)) * 288
    )
    months = months.withColumn(
        "coverage", F.col("valid_slots") / F.col("expected_slots")
    )
    # Add absent months/years for every station to avoid dropping zero-coverage periods.
    years = clean.sparkSession.range(2017, 2025).select(
        F.col("id").cast("int").alias("year")
    )
    calendar_months = clean.sparkSession.range(1, 13).select(
        F.col("id").cast("int").alias("month")
    )
    grid = (
        clean.select("station_id")
        .distinct()
        .crossJoin(years)
        .crossJoin(calendar_months)
    )
    months = (
        grid.join(months, ["station_id", "year", "month"], "left")
        .withColumn(
            "expected_slots",
            F.dayofmonth(F.last_day(F.make_date("year", "month", F.lit(1)))) * 288,
        )
        .fillna({"valid_slots": 0, "observed_days": 0, "coverage": 0.0})
    )
    annual = (
        months.groupBy("station_id", "year")
        .agg(
            F.sum("valid_slots").alias("valid_slots"),
            F.sum("expected_slots").alias("expected_slots"),
        )
        .withColumn("coverage", F.col("valid_slots") / F.col("expected_slots"))
    )
    days = (
        clean.groupBy("station_id", "local_date")
        .agg(F.count("*").alias("valid_slots"))
        .withColumn("complete_day", F.col("valid_slots") == 288)
    )
    spacing_window = Window.partitionBy("station_id").orderBy("epoch")
    gaps = (
        clean.withColumn(
            "elapsed_seconds", F.col("epoch") - F.lag("epoch").over(spacing_window)
        )
        .where("elapsed_seconds > 300")
        .groupBy("year", "elapsed_seconds")
        .count()
    )
    return months, annual, days, gaps
