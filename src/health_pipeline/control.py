"""Business calendar and immutable delivery registration, independent of Airflow."""

import hashlib
import json
import re
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo

SOURCES = {"participants", "device_metadata", "steps", "heart_rate", "sleep", "wellness_survey"}


def parse_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Expected an ISO calendar date")
    return date.fromisoformat(value)


def utc_timestamp(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A timestamp must include a time zone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def daily_scope(trigger_time=None, target_date=None, study_timezone="America/Bogota"):
    zone = ZoneInfo(study_timezone)
    if target_date is not None:
        day = parse_date(target_date)
    else:
        if trigger_time is None:
            raise ValueError("Manual runs require target_date")
        utc_timestamp(trigger_time)
        day = trigger_time.astimezone(zone).date() - timedelta(days=1)
    return {
        "target_date": day.isoformat(),
        "study_timezone": study_timezone,
        "event_period_start": utc_timestamp(datetime.combine(day, time.min, zone)),
        "event_period_end": utc_timestamp(datetime.combine(day + timedelta(days=1), time.min, zone)),
    }


def weekly_scope(trigger_time=None, week_start=None, study_timezone="America/Bogota"):
    zone = ZoneInfo(study_timezone)
    if week_start is not None:
        first = parse_date(week_start)
        if first.weekday() != 0:
            raise ValueError("week_start must be Monday")
    else:
        if trigger_time is None:
            raise ValueError("Manual runs require week_start")
        utc_timestamp(trigger_time)
        local_day = trigger_time.astimezone(zone).date()
        first = local_day - timedelta(days=local_day.weekday() + 7)
    dates = [(first + timedelta(days=i)).isoformat() for i in range(7)]
    return {
        "week_start": first.isoformat(),
        "target_dates": dates,
        "study_timezone": study_timezone,
        "event_period_start": utc_timestamp(datetime.combine(first, time.min, zone)),
        "event_period_end": utc_timestamp(datetime.combine(first + timedelta(days=7), time.min, zone)),
    }


def safe_path(root, key):
    if not isinstance(key, str) or not key or "\\" in key or ":" in key:
        raise ValueError("Expected a safe relative object key")
    parts = PurePosixPath(key)
    if parts.is_absolute() or any(p in {".", ".."} for p in key.split("/")):
        raise ValueError("Object key traversal is prohibited")
    root = Path(root).resolve()
    path = (root / key).resolve()
    if not path.is_relative_to(root) or path == root:
        raise ValueError("Object key is outside the delivery root")
    return path


def validate_manifest(manifest, target_date, study_timezone="America/Bogota"):
    if not isinstance(manifest, dict):
        raise ValueError("A manifest must be an object")
    if type(manifest.get("schema_version")) is not int or manifest.get("schema_version") != 1 or manifest.get("delivery_mode") != "delta":
        raise ValueError("Unsupported manifest schema or delivery mode")
    for name in ("delivery_id", "source_system"):
        if not isinstance(manifest.get(name), str) or not manifest[name].strip():
            raise ValueError(f"Missing {name}")
    if manifest.get("timezone") != study_timezone:
        raise ValueError("Delivery time zone does not match the study")
    dates = manifest.get("target_dates")
    if not isinstance(dates, list) or not dates or len(set(dates)) != len(dates):
        raise ValueError("Invalid target_dates")
    for day in dates:
        parse_date(day)
    parse_date(target_date)
    if target_date not in dates:
        raise ValueError("Delivery does not cover the requested date")
    if manifest.get("batch_kind") not in {"daily", "bootstrap"}:
        raise ValueError("Invalid batch_kind")
    if manifest["batch_kind"] == "daily" and len(dates) != 1:
        raise ValueError("A daily delivery must declare exactly one date")
    utc_timestamp(datetime.fromisoformat(manifest["completed_at"].replace("Z", "+00:00")))
    sources = manifest.get("sources")
    if not isinstance(sources, dict) or set(sources) != SOURCES:
        raise ValueError("All six registered sources must be declared")
    seen = set()
    for source, entry in sources.items():
        if not isinstance(entry, dict):
            raise ValueError("A source declaration must be an object")
        files = entry.get("files")
        if not isinstance(files, list):
            raise ValueError("Source files must be an array")
        if entry.get("status") == "empty_delta":
            if files:
                raise ValueError("An empty delta cannot include files")
        elif entry.get("status") == "files":
            if not files:
                raise ValueError("A file delivery cannot be empty")
        else:
            raise ValueError("Invalid source status")
        for item in files:
            if not isinstance(item, dict):
                raise ValueError("A file declaration must be an object")
            key = item["object_key"]
            safe_path(Path.cwd(), key)
            if key in seen:
                raise ValueError("A file cannot appear twice in one delivery")
            seen.add(key)
            if not re.fullmatch(r"[a-f0-9]{64}", item.get("sha256", "")):
                raise ValueError("Invalid SHA-256 checksum")
            if type(item.get("bytes")) is not int or item["bytes"] < 0:
                raise ValueError("Invalid file size")
            if item.get("format") not in {"csv", "json"}:
                raise ValueError("Invalid file format")
    return manifest


def delivery_available(root, manifest_key, target_date, study_timezone="America/Bogota"):
    path = safe_path(root, manifest_key)
    if not path.is_file():
        return False
    manifest = validate_manifest(json.loads(path.read_text(encoding="utf-8")), target_date, study_timezone)
    for source in manifest["sources"].values():
        for item in source["files"]:
            file = safe_path(root, item["object_key"])
            if not file.is_file():
                return False
            if file.stat().st_size != item["bytes"]:
                raise ValueError("Declared file size does not match the delivery")
    return True


def register_delivery(root, manifest_key, scope, registry_path):
    """Persist verified identity and precedence; do not claim raw ingestion readiness."""
    manifest = validate_manifest(
        json.loads(safe_path(root, manifest_key).read_text(encoding="utf-8")),
        scope["target_date"], scope["study_timezone"],
    )
    for source in manifest["sources"].values():
        for item in source["files"]:
            file = safe_path(root, item["object_key"])
            with file.open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            if checksum != item["sha256"] or file.stat().st_size != item["bytes"]:
                raise ValueError("Declared file content does not match the delivery")
    content = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(content.encode()).hexdigest()
    registry = Path(registry_path)
    registry.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(registry, timeout=30) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS deliveries (
            source_system TEXT NOT NULL, delivery_id TEXT NOT NULL,
            sequence INTEGER NOT NULL, manifest_hash TEXT NOT NULL,
            registered_at TEXT NOT NULL, manifest_json TEXT NOT NULL,
            PRIMARY KEY (source_system, delivery_id), UNIQUE (source_system, sequence))""")
        identity = (manifest["source_system"], manifest["delivery_id"])
        row = db.execute("SELECT sequence, manifest_hash, registered_at FROM deliveries WHERE source_system=? AND delivery_id=?", identity).fetchone()
        if row:
            sequence, previous_hash, registered_at = row
            if previous_hash != digest:
                raise ValueError("A registered delivery_id cannot change content")
        else:
            sequence = db.execute("SELECT COALESCE(MAX(sequence), 0)+1 FROM deliveries WHERE source_system=?", (identity[0],)).fetchone()[0]
            registered_at = utc_timestamp(datetime.now(timezone.utc))
            db.execute("INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?)", (*identity, sequence, digest, registered_at, content))
    return {"source_system": identity[0], "delivery_id": identity[1], "sequence": sequence,
            "manifest_hash": digest, "registered_at": registered_at, "status": "delivery_registered"}
