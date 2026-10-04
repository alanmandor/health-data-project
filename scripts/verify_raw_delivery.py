"""Reconcile a completed raw delivery and verify its captured/archive checksums."""

import argparse
import json
import os
import sqlite3

import duckdb

from health_pipeline.raw_ingestion import checksum, files, identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--delivery-id", required=True)
    args = parser.parse_args()
    registry = os.environ["PROJECT_REGISTRY_PATH"]
    registration = {"source_system": args.source_system, "delivery_id": args.delivery_id, "batch_id": identity(args.source_system, args.delivery_id)}
    declared = files(registry, registration)
    with sqlite3.connect(registry) as control:
        if not control.execute("SELECT 1 FROM deliveries WHERE source_system=? AND delivery_id=?", (args.source_system, args.delivery_id)).fetchone():
            raise ValueError("The requested delivery is not registered")
        receipts = dict(control.execute("SELECT object_key,archive_path FROM archive_receipts WHERE source_system=? AND delivery_id=?", (args.source_system, args.delivery_id)))
    for item in declared:
        if item["verification_status"] != "archived" or item["object_key"] not in receipts:
            raise AssertionError("Delivery is not fully archived")
        if checksum(item["immutable_raw_path"]) != item["checksum"] or checksum(receipts[item["object_key"]]) != item["checksum"]:
            raise AssertionError("Raw or archive checksum changed")
    with duckdb.connect(os.environ["PROJECT_DATABASE_PATH"], read_only=True) as db:
        expected = sum(db.execute("SELECT record_count FROM raw.file_loads WHERE file_id=?", [file_id]).fetchone()[0] for file_id in {item["file_id"] for item in declared})
        actual = db.execute("SELECT count(*) FROM raw.batch_records WHERE batch_id=?", [registration["batch_id"]]).fetchone()[0]
        sources = dict(db.execute("SELECT r.source_name,count(*) FROM raw.source_records r JOIN raw.batch_records b ON b.record_id=r.record_id WHERE b.batch_id=? GROUP BY r.source_name ORDER BY r.source_name", [registration["batch_id"]]).fetchall())
        if actual != expected or sum(sources.values()) != expected:
            raise AssertionError("Raw content, lineage, and file markers do not reconcile")
    print(json.dumps({"delivery_id": args.delivery_id, "archived_files": len(declared), "record_occurrences": actual, "source_counts": sources, "checksums": "verified"}))


if __name__ == "__main__":
    main()
