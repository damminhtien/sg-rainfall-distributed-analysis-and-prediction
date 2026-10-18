"""Small Spark checks for the final probability post-processing helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from pyspark.sql import SparkSession

from rainfall_modeling import interpolate_thresholds, project_nonincreasing


class ProbabilityPostProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("rainfall-modeling-unit-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pava_pools_adjacent_increase_and_returns_monotone_curve(self):
        scores = self.spark.createDataFrame(
            [
                ("S1", 0, 200_000, 0.2, 0.2),
                ("S1", 0, 500_000, 0.5, 0.5),
                ("S1", 0, 1_000_000, 1.0, 0.1),
            ],
            "station_id string, epoch long, threshold_units long, "
            "threshold_mm double, p double",
        )

        result = project_nonincreasing(scores, [0.2, 0.5, 1.0])
        probabilities = [
            row["projected_p"]
            for row in result.orderBy("threshold_units").select("projected_p").collect()
        ]

        self.assertEqual(len(probabilities), 3)
        self.assertAlmostEqual(probabilities[0], 0.35)
        self.assertAlmostEqual(probabilities[1], 0.35)
        self.assertAlmostEqual(probabilities[2], 0.1)
        self.assertTrue(all(a >= b for a, b in zip(probabilities, probabilities[1:])))

    def test_pava_rejects_incomplete_anchor_grid(self):
        scores = self.spark.createDataFrame(
            [
                ("S1", 0, 200_000, 0.2, 0.2),
                ("S1", 0, 1_000_000, 1.0, 0.1),
            ],
            "station_id string, epoch long, threshold_units long, "
            "threshold_mm double, p double",
        )

        with self.assertRaisesRegex(ValueError, "complete ordered anchor grid"):
            project_nonincreasing(scores, [0.2, 0.5, 1.0])

    def test_interpolation_is_linear_inside_anchor_domain(self):
        anchors = self.spark.createDataFrame(
            [
                ("S1", 0, 0.2, 0.4),
                ("S1", 0, 0.5, 0.3),
                ("S1", 0, 1.0, 0.1),
            ],
            "station_id string, epoch long, threshold_mm double, projected_p double",
        )
        requests = self.spark.createDataFrame(
            [("S1", 0, 0.35)],
            "station_id string, epoch long, threshold_mm double",
        )

        result = interpolate_thresholds(
            anchors, requests, [0.2, 0.5, 1.0], requested_thresholds=[0.35]
        ).select("p").first()["p"]

        self.assertAlmostEqual(result, 0.35)

    def test_interpolation_rejects_threshold_outside_anchor_domain(self):
        anchors = self.spark.createDataFrame(
            [("S1", 0, 0.2, 0.4), ("S1", 0, 1.0, 0.1)],
            "station_id string, epoch long, threshold_mm double, projected_p double",
        )
        requests = self.spark.createDataFrame(
            [("S1", 0, 1.1)],
            "station_id string, epoch long, threshold_mm double",
        )

        with self.assertRaisesRegex(ValueError, "inside the anchor domain"):
            interpolate_thresholds(
                anchors, requests, [0.2, 1.0], requested_thresholds=[1.1]
            )


if __name__ == "__main__":
    unittest.main()
