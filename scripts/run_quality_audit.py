"""Run a read-only Spark audit of the annual rainfall CSV files.

Example::

    spark-submit scripts/run_quality_audit.py \
      --manifest manifests/input_manifest.json \
      --source-root "$DATA_ROOT" \
      --output "$OUTPUT_ROOT/quality-audit-RUN_ID"

``--source-root`` is the dataset root containing ``raw/``. It may be a local
directory or a Hadoop-supported URI such as ``gs://bucket-name``. The manifest
must be readable by the Spark driver. Outputs are written to a new directory;
they never modify or clean the input files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

EXPECTED_YEARS = tuple(range(2017, 2025))
EXPECTED_INTERVAL_SECONDS = 300
EXPECTED_SCHEMA = (
    "date",
    "timestamp",
    "update_timestamp",
    "station_id",
    "station_name",
    "station_device_id",
    "location_longitude",
    "location_latitude",
    "reading_update_timestamp",
    "reading_value",
    "reading_type",
    "reading_unit",
)
CORRUPT_RECORD_COLUMN = "_corrupt_record"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Local JSON input manifest readable by the Spark driver.",
    )
    parser.add_argument(
        "--source-root",
        required=True,
        help="Dataset root containing raw/; local path or Hadoop-supported URI.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="New output directory, preferably unique to this run.",
    )
    parser.add_argument(
        "--app-name",
        default="singapore-rainfall-quality-audit",
        help="Spark application name.",
    )
    return parser.parse_args()


def read_manifest(path: Path) -> tuple[dict[str, Any], str]:
    manifest_bytes = path.expanduser().read_bytes()
    manifest = json.loads(manifest_bytes)
    return manifest, hashlib.sha256(manifest_bytes).hexdigest()


def selected_files(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate the frozen eight-file selection before reading any data."""
    selection = manifest.get("selection", {})
    files = manifest.get("files", [])
    if tuple(selection.get("years", ())) != EXPECTED_YEARS:
        raise ValueError(f"Manifest years must be exactly {EXPECTED_YEARS}.")
    if selection.get("missing_expected_files"):
        raise ValueError("Manifest records missing expected annual input files.")

    by_year = {record.get("year"): record for record in files}
    if set(by_year) != set(EXPECTED_YEARS) or len(files) != len(EXPECTED_YEARS):
        raise ValueError("Manifest must contain exactly one input file per year 2017–2024.")

    validated: list[dict[str, Any]] = []
    for year in EXPECTED_YEARS:
        record = by_year[year]
        expected_name = f"HistoricalRainfallacrossSingapore{year}.csv"
        expected_path = PurePosixPath("raw") / expected_name
        relative_path = PurePosixPath(record.get("relative_path", ""))
        if relative_path != expected_path or relative_path.is_absolute():
            raise ValueError(f"Unexpected manifest path for {year}: {relative_path}")
        if record.get("schema") != list(EXPECTED_SCHEMA):
            raise ValueError(f"Manifest schema differs from the expected schema for {year}.")
        if not isinstance(record.get("row_count"), int) or record["row_count"] < 0:
            raise ValueError(f"Manifest row_count is missing or invalid for {year}.")
        if not isinstance(record.get("sha256"), str) or len(record["sha256"]) != 64:
            raise ValueError(f"Manifest SHA-256 is missing or invalid for {year}.")
        validated.append(record)
    return validated


def join_source_path(source_root: str, relative_path: str) -> str:
    """Join a relative manifest path without mangling gs:// or other URIs."""
    return f"{source_root.rstrip('/')}/{relative_path.lstrip('/')}"


def flag_sum(F: Any, condition: Any, name: str) -> Any:
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(name)


def is_blank(F: Any, column_name: str) -> Any:
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def is_finite(F: Any, column: Any) -> Any:
    return (
        column.isNotNull()
        & ~F.isnan(column)
        & (F.abs(column) != F.lit(float("inf")))
    )


def make_input_frame(spark: Any, files: list[dict[str, Any]], source_root: str) -> Any:
    from functools import reduce

    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [StructField(name, StringType(), True) for name in EXPECTED_SCHEMA]
        + [StructField(CORRUPT_RECORD_COLUMN, StringType(), True)]
    )
    reader = (
        spark.read.schema(schema)
        .option("header", "true")
        .option("inferSchema", "false")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .option("encoding", "UTF-8")
    )

    annual_frames = []
    for record in files:
        source_path = join_source_path(source_root, record["relative_path"])
        frame = reader.csv(source_path)
        annual_frames.append(
            frame.withColumn("_source_year", F.lit(record["year"]).cast("int"))
            .withColumn("_source_file", F.lit(record["relative_path"]))
        )

    raw = reduce(lambda left, right: left.unionByName(right), annual_frames)
    # The parsed input is reused by the quality, duplicate, metadata, and
    # spacing summaries. Disk-only persistence avoids keeping the full dataset
    # in executor memory while preventing repeated CSV parsing.
    return raw.persist(StorageLevel.DISK_ONLY)


def add_parsed_columns(F: Any, raw: Any) -> Any:
    parsed = raw
    for column_name in (
        "timestamp",
        "update_timestamp",
        "reading_update_timestamp",
    ):
        parsed = parsed.withColumn(
            f"_{column_name}_parsed", F.to_timestamp(F.trim(F.col(column_name)))
        )
    parsed = parsed.withColumn("_source_date_parsed", F.to_date(F.trim(F.col("date"))))
    for column_name in (
        "reading_value",
        "location_longitude",
        "location_latitude",
    ):
        parsed = parsed.withColumn(
            f"_{column_name}_numeric", F.trim(F.col(column_name)).cast("double")
        )
    return parsed


def annual_quality_summary(F: Any, parsed: Any) -> list[Any]:
    observation_time = F.col("_timestamp_parsed")
    source_date = F.col("_source_date_parsed")
    reading = F.col("_reading_value_numeric")
    longitude = F.col("_location_longitude_numeric")
    latitude = F.col("_location_latitude_numeric")

    observation_missing = is_blank(F, "timestamp")
    date_missing = is_blank(F, "date")
    reading_missing = is_blank(F, "reading_value")
    longitude_missing = is_blank(F, "location_longitude")
    latitude_missing = is_blank(F, "location_latitude")
    finite_reading = is_finite(F, reading)
    finite_longitude = is_finite(F, longitude)
    finite_latitude = is_finite(F, latitude)

    conditions = {
        "csv_parser_corrupt_rows": F.col(CORRUPT_RECORD_COLUMN).isNotNull(),
        "station_id_missing_or_blank": is_blank(F, "station_id"),
        "timestamp_missing": observation_missing,
        "timestamp_unparseable": ~observation_missing & observation_time.isNull(),
        "source_date_missing": date_missing,
        "source_date_unparseable": ~date_missing & source_date.isNull(),
        "source_date_timestamp_mismatch": (
            source_date.isNotNull()
            & observation_time.isNotNull()
            & (source_date != F.to_date(observation_time))
        ),
        "reading_value_missing": reading_missing,
        "reading_value_unparseable": ~reading_missing & reading.isNull(),
        "reading_value_non_finite": reading.isNotNull() & ~finite_reading,
        "reading_value_negative": finite_reading & (reading < 0),
        "reading_unit_missing": is_blank(F, "reading_unit"),
        "reading_unit_not_mm": (
            ~is_blank(F, "reading_unit")
            & (F.upper(F.trim(F.col("reading_unit"))) != "MM")
        ),
        "reading_type_missing": is_blank(F, "reading_type"),
        "update_timestamp_missing": is_blank(F, "update_timestamp"),
        "update_timestamp_unparseable": (
            ~is_blank(F, "update_timestamp") & F.col("_update_timestamp_parsed").isNull()
        ),
        "update_timestamp_before_observation": (
            F.col("_update_timestamp_parsed").isNotNull()
            & observation_time.isNotNull()
            & (F.col("_update_timestamp_parsed") < observation_time)
        ),
        "reading_update_timestamp_missing": is_blank(F, "reading_update_timestamp"),
        "reading_update_timestamp_unparseable": (
            ~is_blank(F, "reading_update_timestamp")
            & F.col("_reading_update_timestamp_parsed").isNull()
        ),
        "reading_update_timestamp_before_observation": (
            F.col("_reading_update_timestamp_parsed").isNotNull()
            & observation_time.isNotNull()
            & (F.col("_reading_update_timestamp_parsed") < observation_time)
        ),
        "longitude_missing": longitude_missing,
        "longitude_unparseable": ~longitude_missing & longitude.isNull(),
        "longitude_non_finite": longitude.isNotNull() & ~finite_longitude,
        "longitude_out_of_range": finite_longitude & ((longitude < -180) | (longitude > 180)),
        "latitude_missing": latitude_missing,
        "latitude_unparseable": ~latitude_missing & latitude.isNull(),
        "latitude_non_finite": latitude.isNotNull() & ~finite_latitude,
        "latitude_out_of_range": finite_latitude & ((latitude < -90) | (latitude > 90)),
    }

    return parsed.groupBy("_source_year").agg(
        F.count(F.lit(1)).cast("long").alias("actual_row_count"),
        *[flag_sum(F, condition, name) for name, condition in conditions.items()],
    ).collect()


def write_json(dataframe: Any, path: str) -> None:
    dataframe.write.mode("errorifexists").json(path)


def assert_output_is_new(spark: Any, output_path: str) -> None:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
    filesystem = hadoop_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    if filesystem.exists(hadoop_path):
        raise FileExistsError(f"Output already exists; choose a new run directory: {output_path}")


def assert_output_outside_raw(source_root: str, output_path: str) -> None:
    """Prevent audit outputs from being written into the immutable raw/ tree."""
    if "://" in source_root:
        raw_root = f"{source_root.rstrip('/')}/raw"
        normalized_output = output_path.rstrip("/")
        if normalized_output == raw_root or normalized_output.startswith(f"{raw_root}/"):
            raise ValueError("--output must be outside the source raw/ directory.")
        return

    raw_root = (Path(source_root).expanduser().resolve() / "raw")
    output = Path(output_path).expanduser()
    if not output.is_absolute():
        output = Path.cwd() / output
    output = output.resolve()
    try:
        output.relative_to(raw_root)
    except ValueError:
        return
    raise ValueError("--output must be outside the source raw/ directory.")


def run_audit(
    spark: Any,
    files: list[dict[str, Any]],
    manifest_sha256: str,
    source_root: str,
    output_path: str,
) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.session.timeZone", "Asia/Singapore")
    # Invalid date/time/numeric values must become null flags rather than stop
    # the audit under an image with ANSI SQL mode enabled.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")

    assert_output_outside_raw(source_root, output_path)
    assert_output_is_new(spark, output_path)
    raw = make_input_frame(spark, files, source_root)
    try:
        parsed = add_parsed_columns(F, raw)

        actual_by_year = {
            int(row["_source_year"]): row.asDict()
            for row in annual_quality_summary(F, parsed)
            if row["_source_year"] is not None
        }
        expected_by_year = {record["year"]: record for record in files}
        unknown_years = set(actual_by_year) - set(expected_by_year)
        if unknown_years:
            raise ValueError(f"Rows came from unexpected source years: {sorted(unknown_years)}")

        summary_rows = []
        for year in EXPECTED_YEARS:
            expected = expected_by_year[year]
            actual = actual_by_year.get(year, {"actual_row_count": 0})
            row = {
                "year": year,
                "relative_path": expected["relative_path"],
                "manifest_sha256": expected["sha256"],
                "expected_row_count": expected["row_count"],
                "actual_row_count": actual.get("actual_row_count", 0),
                "manifest_row_count_matches": (
                    actual.get("actual_row_count", 0) == expected["row_count"]
                ),
            }
            row.update(
                {
                    key: value
                    for key, value in actual.items()
                    if key not in {"_source_year", "actual_row_count"}
                }
            )
            summary_rows.append(row)
        write_json(spark.createDataFrame(summary_rows), f"{output_path.rstrip('/')}/quality_by_year")

        corrupt_candidates = raw.where(F.col(CORRUPT_RECORD_COLUMN).isNotNull()).select(
            F.col("_source_year").alias("source_year"),
            F.col("_source_file").alias("source_file"),
            F.col(CORRUPT_RECORD_COLUMN).alias("raw_corrupt_record"),
            F.lit("spark_csv_parser_corrupt_record").alias("quality_issue_reason"),
        )
        write_json(corrupt_candidates, f"{output_path.rstrip('/')}/csv_parser_corrupt_candidates")

        type_unit_counts = (
            raw.groupBy("_source_year", "reading_type", "reading_unit")
            .count()
            .withColumnRenamed("_source_year", "year")
        )
        write_json(type_unit_counts, f"{output_path.rstrip('/')}/reading_type_unit_distribution")

        station_id = F.trim(F.col("station_id"))
        observation_time = F.col("_timestamp_parsed")
        reading = F.col("_reading_value_numeric")
        finite_reading = is_finite(F, reading)
        valid_key = (
            F.col("station_id").isNotNull()
            & (station_id != "")
            & observation_time.isNotNull()
        )
        duplicate_keys = (
            parsed.where(valid_key)
            .groupBy(station_id.alias("station_id"), observation_time.alias("_observation_time"))
            .agg(
                F.count(F.lit(1)).cast("long").alias("duplicate_row_count"),
                F.countDistinct(F.when(finite_reading, reading)).alias(
                    "distinct_finite_numeric_readings"
                ),
                F.sum(F.when(~finite_reading, 1).otherwise(0))
                .cast("long")
                .alias("non_finite_or_missing_reading_rows"),
                F.min(F.when(finite_reading, reading)).alias("min_finite_numeric_reading"),
                F.max(F.when(finite_reading, reading)).alias("max_finite_numeric_reading"),
                F.countDistinct(F.trim(F.col("timestamp"))).alias("raw_timestamp_variants"),
                F.array_sort(F.collect_set("_source_year")).alias("source_years"),
                F.array_sort(F.collect_set("_source_file")).alias("source_files"),
            )
            .where(F.col("duplicate_row_count") > 1)
            .withColumn(
                "observation_time_sgt",
                F.date_format(F.col("_observation_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            )
            .drop("_observation_time")
            .withColumn(
                "has_conflicting_finite_numeric_readings",
                F.col("distinct_finite_numeric_readings") > 1,
            )
        )
        write_json(duplicate_keys, f"{output_path.rstrip('/')}/duplicate_key_candidates")

        longitude = F.col("_location_longitude_numeric")
        latitude = F.col("_location_latitude_numeric")
        finite_longitude = is_finite(F, longitude)
        finite_latitude = is_finite(F, latitude)
        metadata_variations = (
            parsed.where(F.col("station_id").isNotNull() & (station_id != ""))
            .groupBy(station_id.alias("station_id"))
            .agg(
                F.count(F.lit(1)).cast("long").alias("row_count"),
                F.countDistinct(F.when(~is_blank(F, "station_name"), F.trim(F.col("station_name"))))
                .alias("distinct_nonblank_station_names"),
                F.countDistinct(
                    F.when(~is_blank(F, "station_device_id"), F.trim(F.col("station_device_id")))
                ).alias("distinct_nonblank_device_ids"),
                F.countDistinct(F.when(finite_longitude, longitude)).alias("distinct_finite_longitudes"),
                F.countDistinct(F.when(finite_latitude, latitude)).alias("distinct_finite_latitudes"),
                F.array_sort(F.collect_set("_source_year")).alias("source_years"),
            )
            .where(
                (F.col("distinct_nonblank_station_names") > 1)
                | (F.col("distinct_nonblank_device_ids") > 1)
                | (F.col("distinct_finite_longitudes") > 1)
                | (F.col("distinct_finite_latitudes") > 1)
            )
        )
        write_json(metadata_variations, f"{output_path.rstrip('/')}/station_metadata_variations")

        order = Window.partitionBy(station_id).orderBy(
            observation_time,
            F.col("_source_file"),
            F.col("station_device_id"),
        )
        spacing = (
            parsed.where(valid_key)
            .withColumn("_previous_observation_time", F.lag(observation_time).over(order))
            .withColumn(
                "_elapsed_seconds",
                observation_time.cast("long") - F.col("_previous_observation_time").cast("long"),
            )
            .where(
                F.col("_previous_observation_time").isNotNull()
                & (F.col("_elapsed_seconds") != EXPECTED_INTERVAL_SECONDS)
            )
            .withColumn(
                "spacing_class",
                F.when(F.col("_elapsed_seconds") == 0, "same_timestamp_candidate")
                .when(F.col("_elapsed_seconds") < 0, "negative_ordering_candidate")
                .when(F.col("_elapsed_seconds") < EXPECTED_INTERVAL_SECONDS, "under_5_minutes")
                .otherwise("over_5_minutes"),
            )
            .groupBy(
                F.col("_source_year").alias("source_year"),
                station_id.alias("station_id"),
                F.col("spacing_class"),
                F.col("_elapsed_seconds").alias("elapsed_seconds"),
            )
            .agg(
                F.count(F.lit(1)).cast("long").alias("consecutive_pair_count"),
                F.min(F.date_format(observation_time, "yyyy-MM-dd'T'HH:mm:ssXXX"))
                .alias("first_current_observation_sgt"),
                F.max(F.date_format(observation_time, "yyyy-MM-dd'T'HH:mm:ssXXX"))
                .alias("last_current_observation_sgt"),
            )
        )
        write_json(spacing, f"{output_path.rstrip('/')}/spacing_deviation_distribution")

        metadata = spark.createDataFrame(
            [
                {
                    "audit_version": 1,
                    # Keep this API compatible with older Dataproc Python runtimes.
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
                    "input_manifest_sha256": manifest_sha256,
                    "selected_years": list(EXPECTED_YEARS),
                    "expected_schema": list(EXPECTED_SCHEMA),
                    "timestamp_timezone": "Asia/Singapore",
                    "spacing_reference_seconds": EXPECTED_INTERVAL_SECONDS,
                    "interval_semantics_confirmed": False,
                    "update_field_semantics_confirmed": False,
                    "raw_inputs_modified": False,
                    "conflict_policy_applied": False,
                    "csv_corrupt_record_scope": (
                        "Parser-detected corrupt records only. Spark does not classify rows with "
                        "too few or too many CSV tokens as corrupt; the input manifest scan "
                        "reported zero rows with a wrong field count."
                    ),
                    "interpretation_note": (
                        "Duplicate keys use station_id and parsed timestamp as an audit candidate; "
                        "this does not establish interval-end semantics or justify deduplication. "
                        "Update fields are checked for parseability and chronology only, not treated "
                        "as confirmed publication-availability times. Spacing deviations are not "
                        "automatically classified as missing rainfall. Verify cloud object SHA-256 "
                        "against the manifest before running this job."
                    ),
                }
            ]
        )
        write_json(metadata, f"{output_path.rstrip('/')}/audit_metadata")
    finally:
        raw.unpersist()


def main() -> int:
    args = parse_args()
    manifest, manifest_sha256 = read_manifest(args.manifest)
    files = selected_files(manifest)
    source_root = args.source_root.rstrip("/")
    if not source_root:
        raise SystemExit("--source-root cannot be empty.")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    try:
        run_audit(spark, files, manifest_sha256, source_root, args.output)
    finally:
        spark.stop()
    print(f"Wrote read-only rainfall quality audit outputs to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
