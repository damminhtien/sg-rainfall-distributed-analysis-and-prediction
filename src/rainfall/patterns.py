"""Within-year/across-year aggregates with equal station weighting."""

from pyspark.sql import functions as F


def rainfall_patterns(
    clean, month_coverage, annual_coverage, config, *, include_test=False
):
    if not include_test:
        clean = clean.where("year <= 2023")
        month_coverage = month_coverage.where("year <= 2023")
        annual_coverage = annual_coverage.where("year <= 2023")
    station_month = (
        clean.groupBy("station_id", "year", "month")
        .agg(
            F.sum("rain_mm").alias("observed_total_mm"),
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
            F.avg(F.when(F.col("rain_mm") > 0, F.col("rain_mm"))).alias(
                "wet_intensity_mm"
            ),
            F.count("*").alias("valid_slots"),
        )
        .join(
            month_coverage.select(
                "station_id", "year", "month", "coverage", "expected_slots"
            ),
            ["station_id", "year", "month"],
        )
    )
    monthly = (
        station_month.where(F.col("coverage") >= config["primary_coverage_cutoff"])
        .groupBy("year", "month")
        .agg(
            F.avg("observed_total_mm").alias("mean_observed_total_mm"),
            F.avg("wet_frequency").alias("mean_wet_frequency"),
            F.avg("wet_intensity_mm").alias("mean_wet_intensity_mm"),
            F.avg("coverage").alias("mean_coverage"),
            F.count("*").alias("stations"),
        )
    )
    station_hour = (
        clean.groupBy("station_id", "year", "month", "hour")
        .agg(
            F.avg("rain_mm").alias("mean_mm_per_valid_interval"),
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
            F.count("*").alias("valid_slots"),
        )
        .join(
            month_coverage.select("station_id", "year", "month", "coverage"),
            ["station_id", "year", "month"],
        )
        .where(F.col("coverage") >= config["primary_coverage_cutoff"])
    )
    hourly = station_hour.groupBy("month", "hour").agg(
        F.avg("mean_mm_per_valid_interval").alias("mean_mm_per_valid_interval"),
        F.avg("wet_frequency").alias("mean_wet_frequency"),
        F.count("*").alias("station_year_groups"),
        F.sum("valid_slots").alias("valid_slots"),
    )
    station_annual = (
        clean.groupBy("station_id", "year")
        .agg(
            F.sum("rain_mm").alias("observed_total_mm"),
            F.avg((F.col("rain_mm") > 0).cast("double")).alias("wet_frequency"),
        )
        .join(annual_coverage, ["station_id", "year"])
    )
    expected_years = 8 if include_test else 7
    sensitivity = []
    cohort_support = []
    for cutoff in config["coverage_cutoffs"]:
        common = (
            annual_coverage.groupBy("station_id")
            .agg(
                F.min("coverage").alias("minimum_coverage"),
                F.countDistinct("year").alias("years"),
            )
            .where(
                (F.col("minimum_coverage") >= cutoff)
                & (F.col("years") == expected_years)
            )
        )
        summary = (
            station_annual.join(common.select("station_id"), "station_id")
            .groupBy("year")
            .agg(
                F.avg("observed_total_mm").alias("mean_observed_total_mm"),
                F.avg("wet_frequency").alias("mean_wet_frequency"),
                F.avg("coverage").alias("mean_coverage"),
                F.count("*").alias("common_stations"),
            )
            .withColumn("coverage_cutoff", F.lit(cutoff))
        )
        cohort_support.append(
            common.agg(F.count("*").alias("common_stations"))
            .withColumn("coverage_cutoff", F.lit(cutoff))
            .withColumn("required_years", F.lit(expected_years))
        )
        sensitivity.append(summary)
    annual = sensitivity[0]
    for frame in sensitivity[1:]:
        annual = annual.unionByName(frame)
    support = cohort_support[0]
    for frame in cohort_support[1:]:
        support = support.unionByName(frame)
    descriptive = station_annual.groupBy("year").agg(
        F.avg("observed_total_mm").alias("mean_observed_total_mm"),
        F.avg("coverage").alias("mean_coverage"),
        F.count("*").alias("stations"),
    )
    return {
        "station_month": station_month,
        "monthly": monthly,
        "hourly": hourly,
        "annual_common_cohort": annual,
        "annual_cohort_support": support,
        "annual_descriptive": descriptive,
    }
