"""Reconcile local quality decisions and inspect baseline-specific expectations."""

import argparse
import json
import os

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true", help="Verify the supplied exercise snapshot")
    args = parser.parse_args()
    with duckdb.connect(os.environ["PROJECT_DATABASE_PATH"], read_only=True) as db:
        raw_count = db.execute("SELECT count(*) FROM raw.batch_records").fetchone()[0]
        quality_count = db.execute("SELECT count(*) FROM business.record_quality").fetchone()[0]
        if raw_count != quality_count:
            raise AssertionError("Raw occurrences do not reconcile to quality decisions")
        decisions = db.execute("SELECT source_name, decision, count(*) FROM business.record_quality GROUP BY ALL ORDER BY 1,2").fetchall()
        report = {"raw_occurrences": raw_count, "quality_occurrences": quality_count,
                  "decisions": [{"source": source, "decision": decision, "count": count}
                                for source, decision, count in decisions]}
        if args.baseline:
            if raw_count != 4032814:
                raise AssertionError("Baseline check requires the unchanged exercise snapshot")
            pending = db.execute("SELECT count(*) FROM business.record_quality WHERE source_name='wellness_survey' AND participant_id='P999' AND decision='pending_reference'").fetchone()[0]
            if pending != 14:
                raise AssertionError("P999 surveys must remain recoverable reference quarantine")
            firmware = db.execute("SELECT count(*) FROM business.quality_current WHERE source_name='device_metadata' AND firmware_provenance='device_label'").fetchone()[0]
            if firmware != 15:
                raise AssertionError("Missing firmware recovery does not match the baseline")
            session = db.execute("SELECT record_id, batch_id, sleep_duration_min_reported, efficiency_pct_reported, quality_status, warning_codes FROM business.quality_current WHERE source_name='sleep' AND participant_id='P001' AND observation_date='2026-01-21'").fetchone()
            if session is None or tuple(session[2:5]) != (327, 100, "accepted_with_warning"):
                raise AssertionError("P001 reported sleep values or warning status changed")
            awake = db.execute("SELECT duration_min_reported FROM staging.stg_sleep_stages WHERE session_record_id=? AND batch_id=? AND stage_type='awake'", session[:2]).fetchall()
            if awake != [(55,)]:
                raise AssertionError("Provisional post-boundary awake stage was lost")
            report["baseline"] = {"pending_P999_surveys": pending, "recovered_firmware": firmware,
                                  "P001_sleep_minutes": 327, "P001_efficiency_pct": 100,
                                  "P001_awake_minutes_retained": 55}
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
