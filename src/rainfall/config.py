"""Frozen experiment configuration and portable artifact provenance."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path


def load_config(path):
    config = json.loads(Path(path).read_text())
    if config["years"] != list(range(2017, 2025)):
        raise ValueError("Use exactly 2017–2024.")
    if config["availability_mode"] != "retrospective_event_time":
        raise ValueError(
            "Publication history is not established; only retrospective mode is implemented."
        )
    if config["alignment_tolerance_seconds"] != 0:
        raise ValueError("Only exact five-minute alignment is implemented.")
    thresholds = config["thresholds_mm"]
    if not thresholds or thresholds != sorted(set(thresholds)):
        raise ValueError("Threshold grid must be nonempty, sorted and unique.")
    for key in ("pilot_fraction", "final_train_fraction"):
        if not 0 < config[key] <= 1:
            raise ValueError(f"Invalid {key}.")
    return config


def config_hash(config):
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()


def require_label_semantics(config):
    if not (
        config["interval_semantics_confirmed"]
        or config.get("interval_end_assumption_accepted", False)
    ):
        raise RuntimeError(
            "Interval-end semantics are unverified. Labels/training remain blocked; do not bypass this gate."
        )


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(path)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def freeze_experiment(path, config, *, manifest_sha256, git_commit, artifacts):
    require_label_semantics(config)
    record = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "config": config,
        "config_sha256": config_hash(config),
        "manifest_sha256": manifest_sha256,
        "git_commit": git_commit,
        "artifacts": artifacts,
        "test_accessed": False,
    }
    write_json(path, record)
    return record
