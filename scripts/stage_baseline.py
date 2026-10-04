"""Publish an explicit bootstrap delivery from a downloaded exercise baseline."""

import argparse
import csv
import json
import os
import shutil
import tempfile
from datetime import timedelta
from pathlib import Path

from health_pipeline.control import SOURCES, parse_date, safe_path, validate_manifest
from health_pipeline.raw_ingestion import checksum, identity, immutable_text, now


def stage(source_root, incoming, delivery_id, start, end, study_timezone="America/Bogota"):
    root = Path(source_root)
    first, last = parse_date(start), parse_date(end)
    if last < first:
        raise ValueError("Study end precedes study start")
    participants = root / "participants/participants.csv"
    with participants.open(encoding="utf-8-sig", newline="") as stream:
        identifiers = [row["participant_id"] for row in csv.DictReader(stream)]
    if not identifiers or len(set(identifiers)) != len(identifiers):
        raise ValueError("Baseline participant identifiers must be nonempty and unique")
    expected = {"participants": ["participants/participants.csv"], "device_metadata": ["health_summaries/device_metadata.csv"], "wellness_survey": ["health_summaries/wellness_survey.csv"]}
    for source in ("steps", "heart_rate", "sleep"):
        expected[source] = [f"wearable_events/{participant}/{source}.json" for participant in identifiers]
    for keys in expected.values():
        for key in keys:
            if not safe_path(root, key).is_file():
                raise FileNotFoundError(f"Missing baseline file: {key}")
    prefix = "bootstrap/" + identity("exercise_adapter", delivery_id)
    manifest_path = safe_path(incoming, prefix + "/manifest.json")
    # Published deliveries are immutable. A replay should reuse the existing manifest.
    if manifest_path.exists():
        manifest = validate_manifest(json.loads(manifest_path.read_text()), start, study_timezone)
        if manifest["target_dates"] != [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]:
            raise ValueError("Existing bootstrap scope changed; use a new delivery_id")
        for source, keys in expected.items():
            declared = manifest["sources"][source]["files"]
            if len(declared) != len(keys):
                raise ValueError("Existing bootstrap file coverage changed")
            for key, item in zip(keys, declared):
                if checksum(safe_path(root, key)) != item["sha256"]:
                    raise ValueError("Baseline content changed; use a new delivery_id")
        return prefix + "/manifest.json"
    manifest = {"schema_version": 1, "delivery_id": delivery_id, "source_system": "exercise_adapter", "provenance": "exercise_adapter", "batch_kind": "bootstrap", "delivery_mode": "delta", "target_dates": [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)], "timezone": study_timezone, "completed_at": now(), "sources": {source: {"status": "files", "files": []} for source in SOURCES}}
    for source, keys in expected.items():
        for key in keys:
            destination = safe_path(incoming, prefix + "/" + key)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
                temporary_path = Path(temporary.name)
            try:
                shutil.copyfile(safe_path(root, key), temporary_path)
                if destination.exists():
                    if checksum(destination) != checksum(temporary_path):
                        raise ValueError("An incomplete bootstrap has different content; use a new delivery_id")
                else:
                    os.link(temporary_path, destination)
            finally:
                temporary_path.unlink(missing_ok=True)
            manifest["sources"][source]["files"].append({"object_key": destination.relative_to(incoming).as_posix(), "sha256": checksum(destination), "bytes": destination.stat().st_size, "format": "csv" if key.endswith(".csv") else "json"})
    validate_manifest(manifest, start, study_timezone)
    immutable_text(manifest_path, json.dumps(manifest, sort_keys=True))
    return prefix + "/manifest.json"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--delivery-id", required=True)
    parser.add_argument("--study-start", required=True)
    parser.add_argument("--study-end", required=True, help="Inclusive final study date")
    parser.add_argument("--incoming", default=os.getenv("PROJECT_INCOMING_DIR"))
    parser.add_argument("--study-timezone", default=os.getenv("STUDY_TIMEZONE", "America/Bogota"))
    args = parser.parse_args()
    print(stage(args.source_root, Path(args.incoming), args.delivery_id, args.study_start, args.study_end, args.study_timezone))
