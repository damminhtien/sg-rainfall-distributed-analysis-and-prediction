"""Evidence-oriented EDA: coverage, distributions, calendar patterns and spells."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from .patterns import rainfall_patterns


def explore(clean, month_coverage, annual_coverage, config, *, include_test=False):
    """Return bounded aggregate tables, not collected raw data.

    Wet means >0 mm in a valid five-minute record. Extremes are descriptive
    training wet-reading quantiles, not official warning thresholds. Observed
    contiguous wet spells can be censored by missing data or period boundaries.
    """
    data = clean if include_test else clean.where("year <= 2023")
    monthly_coverage = (
        month_coverage if include_test else month_coverage.where("year <= 2023")
    )
    yearly_coverage = (
        annual_coverage if include_test else annual_coverage.where("year <= 2023")
    )
    outputs = rainfall_patterns(
        clean, month_coverage, annual_coverage, config, include_test=include_test
    )
    train_extreme = (
        clean.where("year <= 2022 AND rain_mm > 0")
        .agg(F.percentile_approx("rain_mm", 0.99, 10000).alias("cutoff"))
        .first()["cutoff"]
    )
    if train_extreme is None:
        raise ValueError(
            "No training wet observations; an extreme reference cannot be defined."
        )
    outputs["distribution_by_year"] = (
        data.groupBy("year")
        .agg(
            F.count("*").alias("valid_intervals"),
            F.sum((F.col("rain_mm") == 0).cast("long")).alias("dry_intervals"),
            F.sum((F.col("rain_mm") > 0).cast("long")).alias("wet_intervals"),
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
            F.avg("rain_mm").alias("mean_mm_per_valid_interval"),
            F.avg(F.when(F.col("rain_mm") > 0, F.col("rain_mm"))).alias(
                "conditional_wet_mean_mm"
            ),
            F.percentile_approx(
                F.when(F.col("rain_mm") > 0, F.col("rain_mm")),
                [0.5, 0.9, 0.95, 0.99],
                10000,
            ).alias("wet_quantiles_mm"),
            F.max("rain_mm").alias("max_observed_mm"),
            F.avg((F.col("rain_mm") > train_extreme).cast("double")).alias(
                "training_wet_p99_exceedance_frequency"
            ),
        )
        .withColumn("training_wet_p99_mm", F.lit(float(train_extreme)))
    )
    bin_expression = F.when(F.col("rain_mm") == 0, F.lit("0: dry"))
    edges = [0.2, 0.5, 1, 2, 5, 10, 20, 50]
    for i, upper in enumerate(edges):
        lower = 0 if i == 0 else edges[i - 1]
        bin_expression = bin_expression.when(
            F.col("rain_mm") <= upper, F.lit(f"{i + 1}: ({lower:g}, {upper:g}]")
        )
    outputs["rainfall_histogram"] = (
        data.withColumn("rainfall_bin", bin_expression.otherwise("9: >50"))
        .groupBy("year", "rainfall_bin")
        .count()
    )
    outputs["coverage_summary"] = yearly_coverage.groupBy("year").agg(
        F.count("*").alias("network_stations"),
        F.sum((F.col("valid_slots") > 0).cast("long")).alias("observed_stations"),
        F.sum("valid_slots").alias("valid_slots"),
        F.sum("expected_slots").alias("fixed_network_calendar_slots"),
        F.avg("coverage").alias("mean_fixed_calendar_coverage"),
        F.min("coverage").alias("minimum_station_coverage"),
        F.max("coverage").alias("maximum_station_coverage"),
    )
    days = (
        data.groupBy("station_id", "year", "month", "local_date")
        .agg(
            F.count("*").alias("valid_slots"),
            F.sum("rain_mm").alias("observed_daily_mm"),
            F.sum((F.col("rain_mm") > 0).cast("long")).alias("wet_intervals"),
        )
        .withColumn("complete_day", F.col("valid_slots") == 288)
    )
    observed_days = days.groupBy("year", "month").agg(
        F.count("*").alias("observed_station_days"),
        F.sum(F.col("complete_day").cast("long")).alias("complete_station_days"),
    )
    calendar_days = monthly_coverage.groupBy("year", "month").agg(
        F.sum(F.col("expected_slots") / 288).alias("fixed_network_calendar_days")
    )
    outputs["daily_completeness"] = (
        calendar_days.join(observed_days, ["year", "month"], "left")
        .fillna(0, ["observed_station_days", "complete_station_days"])
        .withColumn(
            "complete_fraction_of_calendar_days",
            F.col("complete_station_days") / F.col("fixed_network_calendar_days"),
        )
    )
    station_daily = (
        days.where("complete_day")
        .groupBy("station_id", "year", "month")
        .agg(
            F.count("*").alias("complete_station_days"),
            F.avg("observed_daily_mm").alias("mean_complete_day_mm"),
            F.avg((F.col("observed_daily_mm") > 0).cast("double")).alias(
                "wet_complete_day_frequency"
            ),
        )
    )
    outputs["complete_daily_patterns"] = station_daily.groupBy("year", "month").agg(
        F.avg("mean_complete_day_mm").alias("equal_station_mean_complete_day_mm"),
        F.avg("wet_complete_day_frequency").alias(
            "equal_station_wet_complete_day_frequency"
        ),
        F.sum("complete_station_days").alias("complete_station_days"),
        F.count("*").alias("stations"),
    )
    # Fine calendar cycle remains a proportion per valid observation, station weighted.
    station_cycle = (
        data.groupBy("station_id", "year", "month", "hour")
        .agg(
            F.count("*").alias("valid_slots"),
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
            F.avg("rain_mm").alias("mean_mm_per_valid_interval"),
        )
        .join(
            monthly_coverage.select(
                "station_id", "year", "month", "coverage", "expected_slots"
            ),
            ["station_id", "year", "month"],
        )
        .where(F.col("coverage") >= config["primary_coverage_cutoff"])
        .withColumn(
            "hour_coverage", F.col("valid_slots") / (F.col("expected_slots") / 24)
        )
    )
    hours = clean.sparkSession.range(24).select(F.col("id").cast("int").alias("hour"))
    hour_grid = (
        monthly_coverage.where(F.col("coverage") >= config["primary_coverage_cutoff"])
        .select("station_id", "year", "month", "expected_slots")
        .crossJoin(hours)
        .join(
            station_cycle.select("station_id", "year", "month", "hour", "valid_slots"),
            ["station_id", "year", "month", "hour"],
            "left",
        )
        .fillna(0, ["valid_slots"])
        .withColumn(
            "hour_coverage", F.col("valid_slots") / (F.col("expected_slots") / 24)
        )
    )
    outputs["hourly_coverage"] = hour_grid.groupBy("year", "month", "hour").agg(
        F.count("*").alias("eligible_month_station_groups"),
        F.sum(
            (F.col("hour_coverage") >= config["primary_coverage_cutoff"]).cast("long")
        ).alias("eligible_hour_station_groups"),
        F.avg("hour_coverage").alias("mean_hour_coverage"),
        F.min("hour_coverage").alias("minimum_hour_coverage"),
    )
    station_cycle = station_cycle.where(
        F.col("hour_coverage") >= config["primary_coverage_cutoff"]
    )
    outputs["hourly_by_year"] = station_cycle.groupBy("year", "hour").agg(
        F.avg("wet_frequency").alias("equal_station_month_wet_frequency"),
        F.avg("mean_mm_per_valid_interval").alias("equal_station_month_mean_mm"),
        F.count("*").alias("station_month_groups"),
        F.sum("valid_slots").alias("valid_slots"),
    )
    # Override the legacy monthly-only eligibility with explicit hour eligibility.
    outputs["hourly"] = station_cycle.groupBy("month", "hour").agg(
        F.avg("wet_frequency").alias("mean_wet_frequency"),
        F.avg("mean_mm_per_valid_interval").alias("mean_mm_per_valid_interval"),
        F.count("*").alias("station_year_groups"),
        F.sum("valid_slots").alias("valid_slots"),
    )
    outputs["station_spatial_summary"] = (
        data.groupBy("station_id", "year")
        .agg(
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
            F.avg(F.when(F.col("rain_mm") > 0, F.col("rain_mm"))).alias(
                "wet_intensity_mm"
            ),
            F.avg((F.col("rain_mm") > train_extreme).cast("double")).alias(
                "extreme_frequency"
            ),
            F.min("longitude").alias("longitude"),
            F.min("latitude").alias("latitude"),
            F.max("longitude").alias("maximum_longitude"),
            F.max("latitude").alias("maximum_latitude"),
            F.countDistinct(F.struct("longitude", "latitude")).alias(
                "coordinate_positions"
            ),
        )
        .join(
            yearly_coverage.select("station_id", "year", "coverage", "valid_slots"),
            ["station_id", "year"],
        )
    )
    stable_stations = (
        outputs["station_spatial_summary"]
        .groupBy("station_id")
        .agg(
            F.min("longitude").alias("longitude"),
            F.max("maximum_longitude").alias("maximum_longitude"),
            F.min("latitude").alias("latitude"),
            F.max("maximum_latitude").alias("maximum_latitude"),
        )
        .where(
            (F.col("longitude") == F.col("maximum_longitude"))
            & (F.col("latitude") == F.col("maximum_latitude"))
        )
    )
    outputs["station_map"] = (
        outputs["station_spatial_summary"]
        .where(F.col("coverage") >= config["primary_coverage_cutoff"])
        .join(stable_stations.select("station_id"), "station_id")
        .groupBy("station_id")
        .agg(
            F.min("longitude").alias("longitude"),
            F.min("latitude").alias("latitude"),
            F.avg("wet_frequency").alias("mean_annual_wet_frequency"),
            F.avg("wet_intensity_mm").alias("mean_annual_wet_intensity_mm"),
            F.avg("extreme_frequency").alias("mean_annual_extreme_frequency"),
            F.count("*").alias("eligible_years"),
            F.sum("valid_slots").alias("valid_slots"),
        )
    )
    order = Window.partitionBy("station_id").orderBy("epoch")
    spell_frame = (
        data.select("station_id", "year", "epoch", "rain_mm")
        .withColumn("_previous_epoch", F.lag("epoch").over(order))
        .withColumn(
            "_previous_wet", F.lag((F.col("rain_mm") > 0).cast("int")).over(order)
        )
        .withColumn("_next_epoch", F.lead("epoch").over(order))
        .withColumn("_next_wet", F.lead((F.col("rain_mm") > 0).cast("int")).over(order))
    )
    spell_frame = spell_frame.withColumn(
        "_new_spell",
        F.when(
            (F.col("rain_mm") > 0)
            & (
                F.col("_previous_epoch").isNull()
                | (F.col("epoch") - F.col("_previous_epoch") != 300)
                | (F.col("_previous_wet") == 0)
            ),
            1,
        ).otherwise(0),
    )
    spell_frame = spell_frame.withColumn(
        "_spell_id",
        F.sum("_new_spell").over(order.rowsBetween(Window.unboundedPreceding, 0)),
    )
    spells = (
        spell_frame.where("rain_mm > 0")
        .groupBy("station_id", "_spell_id")
        .agg(
            F.min("year").alias("year"),
            F.count("*").alias("intervals"),
            F.sum("rain_mm").alias("observed_spell_mm"),
            F.min("epoch").alias("start_epoch"),
            F.max("epoch").alias("end_epoch"),
            F.max(
                (
                    F.col("_previous_epoch").isNull()
                    | (F.col("epoch") - F.col("_previous_epoch") != 300)
                    | F.col("_next_epoch").isNull()
                    | (F.col("_next_epoch") - F.col("epoch") != 300)
                ).cast("int")
            ).alias("censored_by_gap_or_boundary"),
        )
        .withColumn("observed_duration_minutes", F.col("intervals") * 5)
    )
    outputs["wet_spell_summary"] = spells.groupBy(
        "year", "censored_by_gap_or_boundary"
    ).agg(
        F.count("*").alias("spells"),
        F.avg("observed_duration_minutes").alias("mean_observed_duration_minutes"),
        F.percentile_approx("observed_duration_minutes", [0.5, 0.9, 0.99], 10000).alias(
            "duration_quantiles_minutes"
        ),
        F.max("observed_duration_minutes").alias("max_observed_duration_minutes"),
    )
    return outputs
