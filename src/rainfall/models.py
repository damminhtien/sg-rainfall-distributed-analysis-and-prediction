"""Train-only MLlib pipelines and smoothed empirical-survival climatology."""

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import IsotonicRegression
from pyspark.sql import functions as F

from .features import feature_columns


class Climatology:
    def __init__(self, station_hist, month_hist, global_hist, smoothing):
        self.station_hist = station_hist
        self.month_hist = month_hist
        self.global_hist = global_hist
        self.smoothing = float(smoothing)

    @classmethod
    def fit(cls, train, config):
        def histogram(keys):
            return (
                train.groupBy(*keys, "future_units")
                .count()
                .withColumnRenamed("count", "frequency")
            )

        return cls(
            histogram(["station_id", "month", "hour"]),
            histogram(["month", "hour"]),
            histogram([]),
            config["climatology_smoothing"],
        )

    def transform(self, queries):
        keys = ["station_id", "month", "hour", "threshold_mm", "threshold_units"]
        query = queries.select(*keys).distinct()
        threshold_grid = query.select("threshold_mm", "threshold_units").distinct()
        global_p = (
            threshold_grid.crossJoin(self.global_hist)
            .groupBy("threshold_mm", "threshold_units")
            .agg(
                F.sum("frequency").alias("global_n"),
                F.sum(
                    F.when(
                        F.col("future_units") > F.col("threshold_units"),
                        F.col("frequency"),
                    ).otherwise(0)
                ).alias("global_positives"),
            )
            .withColumn("global_p", F.col("global_positives") / F.col("global_n"))
        )
        broad = (
            query.select("month", "hour", "threshold_mm", "threshold_units")
            .distinct()
            .join(self.month_hist, ["month", "hour"], "left")
            .groupBy("month", "hour", "threshold_mm", "threshold_units")
            .agg(
                F.coalesce(F.sum("frequency"), F.lit(0)).alias("broad_n"),
                F.sum(
                    F.when(
                        F.col("future_units") > F.col("threshold_units"),
                        F.col("frequency"),
                    ).otherwise(0)
                ).alias("broad_positives"),
            )
            .join(
                global_p.select("threshold_mm", "threshold_units", "global_p"),
                ["threshold_mm", "threshold_units"],
            )
            .withColumn(
                "broad_p",
                (F.col("broad_positives") + self.smoothing * F.col("global_p"))
                / (F.col("broad_n") + self.smoothing),
            )
        )
        local = (
            query.join(self.station_hist, ["station_id", "month", "hour"], "left")
            .groupBy(*keys)
            .agg(
                F.coalesce(F.sum("frequency"), F.lit(0)).alias("local_n"),
                F.sum(
                    F.when(
                        F.col("future_units") > F.col("threshold_units"),
                        F.col("frequency"),
                    ).otherwise(0)
                ).alias("local_positives"),
            )
            .join(
                broad.select(
                    "month", "hour", "threshold_mm", "threshold_units", "broad_p"
                ),
                ["month", "hour", "threshold_mm", "threshold_units"],
            )
            .withColumn(
                "p",
                (F.col("local_positives") + self.smoothing * F.col("broad_p"))
                / (F.col("local_n") + self.smoothing),
            )
        )
        return queries.join(local.select(*keys, "p", "local_n"), keys, "left")

    def save(self, path):
        for name, frame in [
            ("station_hist", self.station_hist),
            ("month_hist", self.month_hist),
            ("global_hist", self.global_hist),
        ]:
            frame.write.mode("errorifexists").parquet(f"{path}/{name}")
        self.station_hist.sparkSession.createDataFrame(
            [(self.smoothing,)], "smoothing double"
        ).write.mode("errorifexists").json(f"{path}/metadata")

    @classmethod
    def load(cls, spark, path):
        smoothing = spark.read.json(f"{path}/metadata").first()["smoothing"]
        return cls(
            spark.read.parquet(f"{path}/station_hist"),
            spark.read.parquet(f"{path}/month_hist"),
            spark.read.parquet(f"{path}/global_hist"),
            smoothing,
        )


def pipeline(config, algorithm, parameters, *, variant="B"):
    numeric = feature_columns(config, variant)
    imputed = [f"{name}_imputed" for name in numeric]
    stages = [
        StringIndexer(
            inputCol="station_id", outputCol="station_index", handleInvalid="keep"
        ),
        OneHotEncoder(
            inputCols=["station_index"],
            outputCols=["station_vector"],
            handleInvalid="keep",
            dropLast=False,
        ),
        Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=["station_vector", *imputed], outputCol="assembled"),
    ]
    if algorithm == "lr":
        stages += [
            StandardScaler(
                inputCol="assembled", outputCol="features", withMean=False, withStd=True
            ),
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=50,
                elasticNetParam=0.0,
                **parameters,
            ),
        ]
    elif algorithm == "gbt":
        stages += [
            VectorAssembler(inputCols=["assembled"], outputCol="features"),
            GBTClassifier(
                featuresCol="features",
                labelCol="label",
                seed=config["sampling_seed"],
                maxBins=64,
                **parameters,
            ),
        ]
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    return Pipeline(stages=stages)


def score(model, frame):
    return model.transform(frame).withColumn("p", vector_to_array("probability")[1])


def fit_calibrator(calibration_scores):
    # One pooled monotone mapping handles numeric thresholds absent from training.
    return IsotonicRegression(
        featuresCol="p", labelCol="label", predictionCol="calibrated_p", isotonic=True
    ).fit(calibration_scores)


def calibrated_scores(scores, calibrator):
    return (
        calibrator.transform(scores)
        .withColumnRenamed("p", "raw_p")
        .withColumnRenamed("calibrated_p", "p")
    )


def select_candidate(records, config, primary_thresholds):
    eligible = []
    for record in records:
        supported = [
            r
            for r in record["metrics"]
            if r["threshold_mm"] in primary_thresholds and r["pr_auc"] is not None
        ]
        if supported:
            objective = sum(r["pr_auc"] for r in supported) / len(supported)
        else:
            objective = None
        brier = sum(
            r["brier"] for r in record["metrics"] if r["brier"] is not None
        ) / max(1, sum(r["brier"] is not None for r in record["metrics"]))
        eligible.append((record, objective, brier))
    if not eligible:
        raise ValueError("No model candidates.")
    rankable = [item for item in eligible if item[1] is not None]
    if rankable:
        best = max(item[1] for item in rankable)
        shortlist = [
            item
            for item in rankable
            if best - item[1] <= config["selection_pr_auc_tolerance"]
        ]
    else:
        shortlist = eligible
    minimum_brier = min(item[2] for item in shortlist)
    shortlist = [
        item
        for item in shortlist
        if item[2] - minimum_brier <= config["selection_brier_tolerance"]
    ]
    complexity = {"climatology": 0, "lr": 1, "gbt": 2}
    return min(
        shortlist,
        key=lambda item: (
            complexity[item[0]["algorithm"]],
            item[0].get("candidate_id", ""),
        ),
    )[0]
