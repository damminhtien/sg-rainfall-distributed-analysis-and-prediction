"""Reusable functions to build a manifest from the selected NEA rainfall CSVs.

Run ``build_manifest`` from ``notebooks/00_setup_cloud.ipynb`` so the scanned
file inventory and validation summary are visible with the rest of the project
outputs.
"""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

YEARS = tuple(range(2017, 2025))
FILE_PATTERN = "HistoricalRainfallacrossSingapore{year}.csv"
SOURCE_URL = "https://data.gov.sg/collections/2279/view"
COLLECTION_ID = "2279"
EXPECTED_SCHEMA = [
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
]
DATASET_IDS = {
    2017: "d_1990a5a1aeaf3dd243cf4dae294a61c4",
    2018: "d_024fb501ce7092b71bb713eaf54fa7eb",
    2019: "d_61995f092320e7155b7528050880b502",
    2020: "d_9e7de44094f876f6804b8b5bcee45c81",
    2021: "d_3b41598f74f1f11fc3430348fea51af5",
    2022: "d_42d64cc6c176ace1c52fbb40b9ede302",
    2023: "d_f864cc30d58b467db83659ad17c737bf",
    2024: "d_a0b69d3e02576a1fd0ab673e71f83507",
}


class HashingReader(io.RawIOBase):
    """Hash bytes as a text decoder consumes a file, avoiding a second pass."""

    def __init__(self, source: BinaryIO) -> None:
        self.source = source
        self.digest = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:
        size = self.source.readinto(buffer)
        if size:
            self.digest.update(memoryview(buffer)[:size])
        return size

    def close(self) -> None:
        if not self.closed:
            self.source.close()
        super().close()


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    return timestamp if timestamp.tzinfo is not None else None


def inspect_file(path: Path, year: int) -> dict[str, object]:
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    timestamp_min: datetime | None = None
    timestamp_max: datetime | None = None
    bad_timestamps = 0
    malformed_rows = 0
    row_count = 0
    schema: list[str] = []
    timezone_offsets: set[str] = set()

    with path.open("rb", buffering=0) as binary:
        hashing_reader = HashingReader(binary)
        buffered = io.BufferedReader(hashing_reader)
        with io.TextIOWrapper(buffered, encoding="utf-8-sig", newline="") as text:
            reader = csv.reader(text)
            schema = next(reader, [])
            try:
                timestamp_index = schema.index("timestamp")
            except ValueError:
                timestamp_index = -1

            for row in reader:
                row_count += 1
                if len(row) != len(schema):
                    malformed_rows += 1
                if timestamp_index < 0 or timestamp_index >= len(row):
                    bad_timestamps += 1
                    continue

                timestamp_text = row[timestamp_index].strip()
                timestamp = parse_timestamp(timestamp_text)
                if timestamp is None:
                    bad_timestamps += 1
                    continue
                if first_timestamp is None:
                    first_timestamp = timestamp
                last_timestamp = timestamp
                timestamp_min = (
                    timestamp
                    if timestamp_min is None
                    else min(timestamp_min, timestamp)
                )
                timestamp_max = (
                    timestamp
                    if timestamp_max is None
                    else max(timestamp_max, timestamp)
                )
                offset = timestamp.strftime("%z")
                timezone_offsets.add(
                    f"{offset[:3]}:{offset[3:]}" if offset else "naive"
                )

        sha256 = hashing_reader.digest.hexdigest()

    return {
        "year": year,
        "dataset_id": DATASET_IDS[year],
        "dataset_url": f"https://data.gov.sg/datasets/{DATASET_IDS[year]}/view",
        "relative_path": f"raw/{path.name}",
        "size_bytes": path.stat().st_size,
        "sha256": sha256,
        "row_count": row_count,
        "schema": schema,
        "timestamp_column": "timestamp" if "timestamp" in schema else None,
        "timestamp_first_row": first_timestamp.isoformat() if first_timestamp else None,
        "timestamp_last_row": last_timestamp.isoformat() if last_timestamp else None,
        "timestamp_min": timestamp_min.isoformat() if timestamp_min else None,
        "timestamp_max": timestamp_max.isoformat() if timestamp_max else None,
        "timestamp_timezone_offsets": sorted(timezone_offsets),
        "unparseable_or_missing_timestamps": bad_timestamps,
        "rows_with_wrong_field_count": malformed_rows,
    }


def build_manifest(data_root: Path) -> dict[str, object]:
    raw_root = data_root / "raw"
    files = []
    missing = []
    for year in YEARS:
        path = raw_root / FILE_PATTERN.format(year=year)
        if not path.is_file():
            missing.append(path.name)
            continue
        files.append(inspect_file(path, year))

    first_schema = files[0]["schema"] if files else []
    for record in files:
        record["schema_matches_first_file"] = record["schema"] == first_schema
        record["schema_matches_expected"] = record["schema"] == EXPECTED_SCHEMA

    extra_csvs = sorted(
        path.name
        for path in raw_root.glob("*.csv")
        if path.name not in {FILE_PATTERN.format(year=year) for year in YEARS}
    )
    return {
        "manifest_version": 1,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "source": {
            "name": "Historical Rainfall across Singapore",
            "collection_id": COLLECTION_ID,
            "url": SOURCE_URL,
            "timestamp_timezone": "Asia/Singapore",
            "timestamp_format": "ISO 8601",
            "retrieval_time": None,
            "retrieval_time_note": "Original download time is not available locally.",
        },
        "selection": {
            "years": list(YEARS),
            "excluded_extra_csvs": extra_csvs,
            "missing_expected_files": missing,
        },
        "files": files,
    }
