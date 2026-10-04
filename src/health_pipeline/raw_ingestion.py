"""Immutable capture, transactional raw ingestion, and recoverable archival."""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from health_pipeline.control import register_delivery, safe_path, utc_timestamp, validate_manifest


def identity(*parts):
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def checksum(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def now():
    return utc_timestamp(datetime.now(timezone.utc))


def immutable_text(path, content):
    """Publish completed metadata without overwriting an existing identity."""
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False, mode="w", encoding="utf-8") as output:
        temporary = Path(output.name)
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    try:
        if path.exists():
            if path.read_text() != content:
                raise ValueError("Immutable metadata cannot change")
        else:
            os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def manifest_location(root, key, raw):
    """Prefer a current manifest; fall back to its last immutable capture on replay."""
    incoming = safe_path(root, key)
    if incoming.exists():
        return incoming
    pointer = Path(raw) / "manifest_keys" / identity(key)
    if not pointer.exists():
        return incoming
    return safe_path(raw, pointer.read_text())


def available(root, manifest_key, target_date, study_timezone, raw):
    path = manifest_location(root, manifest_key, raw)
    if not path.is_file():
        return False
    manifest = validate_manifest(json.loads(path.read_text()), target_date, study_timezone)
    capture = Path(raw) / "deliveries" / identity(manifest["source_system"], manifest["delivery_id"])
    for entry in manifest["sources"].values():
        for item in entry["files"]:
            cached = safe_path(capture / "files", item["object_key"])
            file = cached if cached.is_file() else safe_path(root, item["object_key"])
            if not file.is_file():
                return False
            if file.stat().st_size != item["bytes"]:
                raise ValueError("Declared file size does not match delivery")
    return True


def initialize(database, registry, ddl_root):
    import duckdb

    Path(database).parent.mkdir(parents=True, exist_ok=True)
    Path(registry).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as db:
        db.execute((Path(ddl_root) / "duckdb/001_foundation.sql").read_text())
    with sqlite3.connect(registry) as db:
        db.executescript((Path(ddl_root) / "sqlite/001_control_registry.sql").read_text())
    return {"status": "warehouse_initialized"}


def capture(root, manifest_key, scope, raw, registry, run_key=None):
    manifest_path = manifest_location(root, manifest_key, raw)
    if run_key is not None:
        binding = Path(raw) / "run_bindings" / identity(run_key)
        binding.parent.mkdir(parents=True, exist_ok=True)
        if not binding.exists():
            candidate = validate_manifest(json.loads(manifest_path.read_text()), scope["target_date"], scope["study_timezone"])
            immutable_text(binding, json.dumps(candidate, sort_keys=True))
        manifest_path = binding
    manifest = validate_manifest(json.loads(manifest_path.read_text()), scope["target_date"], scope["study_timezone"])
    batch = identity(manifest["source_system"], manifest["delivery_id"])
    folder = Path(raw) / "deliveries" / batch
    folder.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    if any(item["object_key"] == "_control_manifest.json" for entry in manifest["sources"].values() for item in entry["files"]):
        raise ValueError("Reserved capture manifest filename")
    saved = folder / "manifest.json"
    if saved.exists() and saved.read_text() != serialized:
        raise ValueError("An immutable captured delivery cannot change")
    for source, entry in manifest["sources"].items():
        for item in entry["files"]:
            destination = safe_path(folder / "files", item["object_key"])
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists():
                with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
                    temporary = Path(output.name)
                    try:
                        with safe_path(root, item["object_key"]).open("rb") as stream:
                            while block := stream.read(1024 * 1024):
                                output.write(block)
                        output.flush()
                        os.fsync(output.fileno())
                    except BaseException:
                        temporary.unlink(missing_ok=True)
                        raise
                try:
                    if checksum(temporary) != item["sha256"] or temporary.stat().st_size != item["bytes"]:
                        raise ValueError("Capture checksum or size mismatch")
                    os.link(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
            if checksum(destination) != item["sha256"] or destination.stat().st_size != item["bytes"]:
                raise ValueError("Immutable raw capture failed verification")
    immutable_text(saved, serialized)
    registration_root = folder / "files"
    registration_root.mkdir(exist_ok=True)
    registration_manifest = registration_root / "_control_manifest.json"
    immutable_text(registration_manifest, serialized)
    registered = register_delivery(registration_root, "_control_manifest.json", scope, registry)
    with sqlite3.connect(registry) as db:
        for source, entry in manifest["sources"].items():
            for item in entry["files"]:
                file_id = identity(manifest["source_system"], source, item["format"], item["sha256"])
                db.execute("""INSERT OR IGNORE INTO delivery_files
                    (source_system,delivery_id,object_key,file_id,checksum,bytes,format,version_id,source_name,immutable_raw_path,verification_status,verified_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (manifest["source_system"], manifest["delivery_id"], item["object_key"], file_id, item["sha256"], item["bytes"], item["format"], item.get("version_id"), source, str(safe_path(registration_root, item["object_key"])), "captured", now()))
    pointers = Path(raw) / "manifest_keys"
    pointers.mkdir(exist_ok=True)
    pointer = pointers / identity(manifest_key)
    temporary_pointer = pointer.with_suffix(".tmp")
    temporary_pointer.write_text(saved.relative_to(raw).as_posix())
    os.replace(temporary_pointer, pointer)
    return {**registered, "batch_id": batch}


def file_rows(path, format):
    text = Path(path).read_text(encoding="utf-8-sig")
    if format == "json":
        payload = json.loads(text)
        if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
            raise ValueError("JSON source must be an array of record objects")
        for position, row in enumerate(payload, 1):
            yield position, json.dumps(row, ensure_ascii=False, allow_nan=False), None
    else:
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        header = next(reader, None)
        if not header or len(set(header)) != len(header) or any(not field for field in header):
            raise ValueError("CSV requires nonempty unique column names")
        lines = text.splitlines(keepends=True)
        previous = reader.line_num
        for position, values in enumerate(reader, 1):
            if len(values) != len(header):
                raise ValueError("CSV row width does not match its header")
            original = "".join(lines[previous:reader.line_num])
            previous = reader.line_num
            yield position, json.dumps(dict(zip(header, values)), ensure_ascii=False), original


def files(registry, registration):
    with sqlite3.connect(registry) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM delivery_files WHERE source_system=? AND delivery_id=? ORDER BY source_name,object_key", (registration["source_system"], registration["delivery_id"]))]


def load(database, registry, registration):
    import duckdb

    total = 0
    inserted_at = now()
    def insert_chunk(db, rows):
        # Bind one JSON string rather than infer thousands of Python STRUCT fields.
        expressions = ",".join(f"json_extract_string(value,'$[{i}]')" for i in range(7))
        db.execute(f"INSERT INTO raw.source_records SELECT {expressions} FROM json_each(?)", [json.dumps(rows, ensure_ascii=False)])
    with duckdb.connect(str(database)) as db:
        db.execute("BEGIN")
        try:
            # Any lineage row proves the entire batch committed: raw load is atomic.
            batch_loaded = db.execute("SELECT EXISTS(SELECT 1 FROM raw.batch_records WHERE batch_id=? LIMIT 1)", [registration["batch_id"]]).fetchone()[0]
            for item in files(registry, registration):
                path = item["immutable_raw_path"]
                if checksum(path) != item["checksum"]:
                    raise ValueError("Captured content changed before raw load")
                marker = db.execute("SELECT record_count FROM raw.file_loads WHERE file_id=?", [item["file_id"]]).fetchone()
                if not marker:
                    count = 0
                    chunk = []
                    for position, payload, original in file_rows(path, item["format"]):
                        chunk.append((identity(item["file_id"], position), item["file_id"], item["source_name"], position, payload, original, inserted_at))
                        count += 1
                        if len(chunk) == 1000:
                            insert_chunk(db, chunk)
                            chunk.clear()
                    if chunk:
                        insert_chunk(db, chunk)
                    db.execute("INSERT INTO raw.file_loads VALUES (?,?,?)", [item["file_id"], count, now()])
                else:
                    count = marker[0]
                if not batch_loaded:
                    db.execute("""INSERT INTO raw.batch_records SELECT ?,record_id,?,?,? FROM raw.source_records WHERE file_id=?
                        ON CONFLICT DO NOTHING""", [registration["batch_id"], registration["source_system"], registration["sequence"], item["object_key"], item["file_id"]])
                total += count
                logging.getLogger(__name__).info("Raw file prepared: source=%s file_id=%s records=%s reused=%s", item["source_name"], item["file_id"], count, bool(marker))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    # A crash here is repaired on retry using the transactional DuckDB markers.
    with sqlite3.connect(registry) as db:
        db.execute("UPDATE delivery_files SET verification_status='raw_loaded' WHERE source_system=? AND delivery_id=? AND verification_status <> 'archived'", (registration["source_system"], registration["delivery_id"]))
    return {**registration, "status": "raw_loaded", "record_occurrences": total}


def archive(root, archive_root, registry, registration):
    for item in files(registry, registration):
        if item["verification_status"] == "archived":
            continue
        if item["verification_status"] != "raw_loaded":
            raise ValueError("Archive requires confirmed raw ingestion")
        source = safe_path(root, item["object_key"])
        original_key = Path(item["object_key"])
        destination = safe_path(Path(archive_root) / registration["batch_id"], (original_key.parent / f"{original_key.stem}__{item['checksum']}{original_key.suffix}").as_posix())
        destination.parent.mkdir(parents=True, exist_ok=True)
        claim = destination.with_name(destination.name + ".claim")
        if not destination.exists():
            if not claim.exists():
                # Atomic claim first: verify the moved inode, not a prior path check.
                if not source.exists():
                    raise FileNotFoundError("Input disappeared before archival")
                os.rename(source, claim)
            if checksum(claim) != item["checksum"]:
                # Restore without overwriting a newer producer file.
                try:
                    os.link(claim, source)
                    claim.unlink()
                except FileExistsError:
                    pass
                raise ValueError("Input was replaced; preserved replacement and refused archival")
            os.link(claim, destination)
            claim.unlink()
        if checksum(destination) != item["checksum"]:
            raise ValueError("Archive checksum mismatch")
        if claim.exists() and checksum(claim) == item["checksum"]:
            claim.unlink()
        with sqlite3.connect(registry) as db:
            db.execute("INSERT OR IGNORE INTO archive_receipts VALUES (?,?,?,?,?)", (registration["source_system"], registration["delivery_id"], item["object_key"], str(destination), now()))
            db.execute("UPDATE delivery_files SET verification_status='archived' WHERE source_system=? AND delivery_id=? AND object_key=?", (registration["source_system"], registration["delivery_id"], item["object_key"]))
    return {**registration, "status": "raw_archived"}


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["initialize", "load"])
    parser.add_argument("--registration", default="{}")
    args = parser.parse_args()
    database = os.environ["PROJECT_DATABASE_PATH"]
    registry = os.environ.get("PROJECT_REGISTRY_PATH", "/opt/airflow/project-data/control/deliveries.sqlite")
    if args.action == "initialize":
        result = initialize(database, registry, os.environ.get("PROJECT_DDL_ROOT", "/opt/airflow/project/ddl"))
    else:
        result = load(database, registry, json.loads(args.registration))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
