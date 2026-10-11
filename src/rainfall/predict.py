"""Portable station/time/threshold probability inference from frozen artifacts."""

import json
import math
from datetime import datetime
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.regression import IsotonicRegressionModel
from pyspark.sql import functions as F

from .features import build_base_features, expand_thresholds
from .models import Climatology, calibrated_scores, score


class RainfallPredictor:
    def __init__(self, model, metadata, history, stations, calibrator=None):
        self.model = model
        self.metadata = metadata
        self.history = history
        self.stations = stations
        self.calibrator = calibrator

    @classmethod
    def load(cls, spark, package_path, history):
        """Package paths work after download/extraction, without access to personal GCS."""
        root = Path(package_path)
        metadata = json.loads((root / "metadata.json").read_text())
        if metadata["algorithm"] == "climatology":
            model = Climatology.load(spark, str(root / "selected"))
        else:
            model = PipelineModel.load(str(root / "selected"))
        calibrator = (
            IsotonicRegressionModel.load(str(root / "calibrator"))
            if metadata["use_calibration"]
            else None
        )
        stations = spark.read.parquet(str(root / "training_stations"))
        return cls(model, metadata, history, stations, calibrator)

    def predict(self, station_id, prediction_time, threshold_mm):
        result = {
            "station_id": station_id,
            "prediction_time": prediction_time,
            "threshold_mm": threshold_mm,
            "probability": None,
            "mode": self.metadata["config"]["availability_mode"],
        }
        if not isinstance(station_id, str) or not station_id.strip():
            return dict(result, status="invalid_station")
        try:
            time = datetime.fromisoformat(prediction_time)
            if time.tzinfo is None:
                raise ValueError("Timezone is required.")
            epoch = int(time.timestamp())
            threshold = float(threshold_mm)
            if time.microsecond or time.second or epoch % 300:
                return dict(result, status="off_grid_time")
        except (ValueError, TypeError, OverflowError):
            return dict(result, status="invalid_time_or_threshold")
        config = self.metadata["config"]
        low, high = config["threshold_domain_mm"]
        if not math.isfinite(threshold) or not low <= threshold <= high:
            return dict(result, status="threshold_out_of_domain")
        station_id = station_id.strip()
        history = self.history.where(
            (F.col("station_id") == station_id)
            & (F.col("epoch") >= epoch - 10800)
            & (F.col("epoch") <= epoch)
        ).select("station_id", "epoch", "rain_mm", "rain_units")
        if not history.limit(1).count():
            return dict(result, status="insufficient_history")
        current_missing = not history.where(F.col("epoch") == epoch).limit(1).count()
        if current_missing:
            query = history.sparkSession.createDataFrame(
                [(station_id, epoch, None, None)],
                "station_id string, epoch long, rain_mm double, rain_units long",
            )
            history = history.unionByName(query)
        base = build_base_features(history, config, with_labels=False).where(
            F.col("epoch") == epoch
        )
        queries = expand_thresholds(base, config, thresholds=[threshold])
        prediction = (
            self.model.transform(queries)
            if self.metadata["algorithm"] == "climatology"
            else score(self.model, queries)
        )
        if self.calibrator is not None:
            prediction = calibrated_scores(prediction, self.calibrator)
        probability = prediction.select("p").first()["p"]
        if (
            probability is None
            or not math.isfinite(probability)
            or not 0 <= probability <= 1
        ):
            raise ValueError("Model returned an invalid probability.")
        unseen = (
            not self.stations.where(F.col("station_id") == station_id).limit(1).count()
        )
        status = (
            "unseen_station"
            if unseen
            else ("missing_current_observation" if current_missing else "ok")
        )
        return dict(
            result,
            probability=float(probability),
            status=status,
            unseen_station=unseen,
            current_missing=current_missing,
        )
