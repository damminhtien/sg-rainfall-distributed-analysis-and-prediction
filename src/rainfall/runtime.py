"""Run metadata and Spark job evidence for notebook cells."""

import json
import platform
import time
import warnings
from contextlib import contextmanager
from datetime import UTC, datetime

from IPython.display import Markdown, display


@contextmanager
def spark_action(spark, run_id, action):
    group = f"{run_id}:{action}:{time.time_ns()}"
    started = time.monotonic()
    spark.sparkContext.setJobGroup(group, action, True)
    completed = False
    try:
        yield
        completed = True
    finally:
        tracker = spark.sparkContext.statusTracker()
        jobs = []
        for job_id in sorted(tracker.getJobIdsForGroup(group)):
            info = tracker.getJobInfo(job_id)
            jobs.append(
                {"job_id": job_id, "status": info.status if info else "unavailable"}
            )
        record = {
            "run_id": run_id,
            "action": action,
            "job_group": group,
            "application_id": spark.sparkContext.applicationId,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "duration_seconds": round(time.monotonic() - started, 3),
            "completed": completed,
            "jobs": jobs,
            "spark_ui": spark.sparkContext.uiWebUrl,
        }
        evidence_root = spark.conf.get("rainfall.evidence.root", "")
        if evidence_root.startswith("gs://"):
            try:
                from google.cloud import storage

                bucket_name, prefix = evidence_root[5:].split("/", 1)
                object_name = f"{prefix}/{action}-{time.time_ns()}.json"
                storage.Client().bucket(bucket_name).blob(
                    object_name
                ).upload_from_string(
                    json.dumps(record), content_type="application/json"
                )
            except Exception:  # noqa: BLE001 — preserve the original Spark exception.
                # Preserve the analysis exception and the displayed evidence record.
                record["evidence_upload_failed"] = True
                warnings.warn(
                    "Evidence upload failed; preserve the notebook record.",
                    stacklevel=2,
                )
        display(record)
        display(
            Markdown(
                "Review the **Jobs summary and every job detail** in Spark Web UI, then capture original screenshots."
            )
        )
        for property_name in (
            "spark.jobGroup.id",
            "spark.job.description",
            "spark.job.interruptOnCancel",
        ):
            spark.sparkContext.setLocalProperty(property_name, None)


def environment(spark):
    conf = dict(spark.sparkContext.getConf().getAll())
    return {
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "master": spark.sparkContext.master,
        "application_id": spark.sparkContext.applicationId,
        "event_logging": conf.get("spark.eventLog.enabled", "false"),
        "event_log_dir": conf.get("spark.eventLog.dir"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
    }


def require_distributed(spark):
    if spark.sparkContext.master != "yarn":
        raise RuntimeError(
            "Run this notebook on Dataproc YARN, not a local Spark cluster."
        )
    if spark.sparkContext.getConf().get("spark.eventLog.enabled", "false") != "true":
        raise RuntimeError("Enable Spark event logging before executing analysis.")
