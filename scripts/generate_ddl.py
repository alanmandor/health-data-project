"""Generate reviewed foundation DDL for DuckDB, Snowflake, and SQLite controls."""

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TYPES = {
    "duckdb": {"s": "VARCHAR", "i": "BIGINT", "h": "SMALLINT", "n": "DECIMAL(18,6)", "d": "DATE", "t": "TIMESTAMPTZ", "j": "JSON", "b": "BOOLEAN"},
    "snowflake": {"s": "VARCHAR", "i": "NUMBER(38,0)", "h": "NUMBER(5,0)", "n": "NUMBER(18,6)", "d": "DATE", "t": "TIMESTAMP_TZ(6)", "j": "VARIANT", "b": "BOOLEAN"},
}
TABLES = []
QUALITY_CATALOGS = {
    'quality_issue_statuses': (
        ('open', 'Open', 'The issue remains active and requires resolution.'),
        ('resolved', 'Resolved', 'The issue was closed through an audited lifecycle transition.'),
    ),
    'quality_severities': (
        ('error', 'Error', 'The record is rejected or conflicting and is excluded from accepted output.'),
        ('warning', 'Warning', 'The record is accepted with a condition requiring review.'),
        ('pending', 'Pending', 'The record awaits an authoritative reference before acceptance.'),
    ),
    'quality_resolution_codes': (
        ('reference_resolved', 'Reference resolved', 'The missing authoritative reference became available.'),
        ('superseded_by_accepted_delta', 'Superseded by accepted delta', 'A later accepted upsert replaced the affected record.'),
        ('source_deleted', 'Source deleted', 'An accepted explicit delete removed the current business key.'),
        ('rule_cleared', 'Rule cleared', 'The original issue condition no longer applies.'),
        ('excluded_at_consolidation', 'Excluded at consolidation', 'A permitted terminal awake interval was excluded from a frozen publication.'),
        ('reopened', 'Reopened', 'A previously resolved issue became active again; its current status is open.'),
    ),
}


def table(name, columns, pk, unique=(), checks=()):
    TABLES.append((name, columns.split(), pk, unique, checks))


HUB = "first_batch_id:s! record_source:s! loaded_at:t! hash_spec_version:s!"
SAT = "version_id:s! parent_hk:s! batch_id:s! delivery_sequence:i! record_source:s! loaded_at:t! source_effective_at:t payload_hash:s! hash_spec_version:s! operation:s!"
LINEAGE = "source_version_id:s! quality_status:s! rule_version:s!"
SCORES = "fatigue_score:h stress_score:h readiness_score:h sleep_quality_score:h"

table("raw.source_records", "record_id:s! file_id:s! source_name:s! row_number:i! payload_json:j! original_record_text:s ingested_at:t!", "record_id", checks=("row_number >= 0",))
table("raw.batch_records", "batch_id:s! record_id:s! source_system:s! delivery_sequence:i! object_key:s!", "batch_id, record_id")
table("raw.file_loads", "file_id:s! record_count:i! loaded_at:t!", "file_id", checks=("record_count >= 0",))

for name, key, fields, unique in [
    ("participant", "participant_hk", "business_namespace:s! participant_id:s!", "business_namespace, participant_id"),
    ("device", "device_hk", "business_namespace:s! device_id:s!", "business_namespace, device_id"),
    ("study", "study_hk", "study_code:s!", "study_code"),
    ("sleep_session", "sleep_session_hk", "study_hk:s! participant_hk:s! wake_date:d!", "study_hk, participant_hk, wake_date"),
    ("wellness_response", "wellness_response_hk", "study_hk:s! participant_hk:s! survey_date:d!", "study_hk, participant_hk, survey_date"),
]:
    table("vault.hub_" + name, f"{key}:s! {fields} {HUB}", key, unique=(unique,))

for name, key, fields in [
    ("participant_device", "participant_device_hk", "participant_hk:s! device_hk:s!"),
    ("participant_study", "participant_study_hk", "participant_hk:s! study_hk:s!"),
    ("participant_sleep_session", "participant_sleep_hk", "participant_hk:s! sleep_session_hk:s!"),
    ("participant_wellness_response", "participant_wellness_hk", "participant_hk:s! wellness_response_hk:s!"),
]:
    endpoints = ", ".join(c.split(":")[0] for c in fields.split())
    table("vault.link_" + name, f"{key}:s! {fields} {HUB}", key, unique=(endpoints,))

for name, fields in [
    ("participant_profile", "height_cm:n gender:s chronotype:s"),
    ("device_metadata", "calibration_date:d firmware_version_reported:s device_label:s"),
    ("participant_device", "wear_site:s assignment_status:s effective_from:t effective_to:t"),
    ("study", "study_timezone:s study_start:d study_end_exclusive:d source_registry_version:s"),
    ("enrollment", "age_at_enrollment:h max_heart_rate:h enrollment_status:s enrolled_on:d withdrawn_on:d coverage_start:d coverage_end_exclusive:d coverage_basis:s"),
    ("steps_observation", "study_hk:s! event_at_utc:t! timestamp_original:s steps_reported:i unit_reported:s"),
    ("heart_rate_observation", "study_hk:s! event_at_utc:t! timestamp_original:s heart_rate_bpm:i"),
    ("sleep_session", "sleep_onset_utc:t sleep_end_utc:t sleep_onset_original:s sleep_end_original:s efficiency_pct_reported:n restlessness:n"),
    ("wellness_response", SCORES),
]:
    table("vault.sat_" + name, SAT + " " + fields, "version_id", checks=("operation IN ('upsert', 'delete')",))
table("vault.sat_sleep_stage", "session_version_id:s! stage_position:i! stage_type:s! start_at_utc:t end_at_utc:t start_original:s end_original:s duration_min_reported:n", "session_version_id, stage_position", checks=("stage_position >= 0",))
table("vault.version_lineage", "source_relation:s! version_id:s! batch_id:s! record_id:s!", "source_relation, version_id, batch_id, record_id")

table("business.version_decisions", "decision_id:s! version_id:s! source_relation:s! business_key_hash:s! study_hk:s! decision:s! decided_at:t! rule_version:s! batch_id:s! reason_code:s normalized_payload_hash:s", "decision_id", checks=("decision IN ('accepted', 'rejected', 'conflict', 'pending_reference')",))
table("business.vault_loads", "evaluation_id:s! input_hash:s! quality_code_version:s! loader_code_version:s! study_hk:s! committed_at:t!", "evaluation_id")
for catalog in QUALITY_CATALOGS:
    table('business.' + catalog, 'code:s! display_name:s! description:s!', 'code')

table("business.quality_issues", "issue_id:s! batch_id:s! record_id:s version_id:s rule_code:s! severity:s! first_seen_at:t! current_status:s!", "issue_id", checks=(
    "severity IN ('error', 'warning', 'pending')",
    "current_status IN ('open', 'resolved')",
))
table("business.quality_resolutions", "resolution_id:s! issue_id:s! resolved_at:t! resolution_code:s! superseding_version_id:s publication_id:s", "resolution_id", checks=(
    "resolution_code IN ('reference_resolved', 'superseded_by_accepted_delta', 'source_deleted', 'rule_cleared', 'excluded_at_consolidation', 'reopened')",
))
table("business.quality_evaluations", "evaluation_id:s! vault_evaluation_id:s! implementation_hash:s! committed_at:t!", "evaluation_id")
table("business.affected_participant_dates", "study_hk:s! participant_hk:s! local_date:d! change_id:s! reason_code:s! created_at:t! consumed_checkpoint_id:s", "study_hk, participant_hk, local_date, change_id")
table("business.participant_date_state", "study_hk:s! participant_hk:s! local_date:d! state_hash:s! evaluation_id:s!", "study_hk, participant_hk, local_date")
table("business.impact_evaluations", "evaluation_id:s! vault_evaluation_id:s! quality_evaluation_id:s! implementation_hash:s! affected_date_count:i! committed_at:t!", "evaluation_id")
table("business.mart_evaluations", "evaluation_id:s! impact_evaluation_id:s! implementation_hash:s! affected_date_count:i! committed_at:t!", "evaluation_id")
table("business.mart_build_scope", "study_hk:s! participant_hk:s! local_date:d!", "study_hk, participant_hk, local_date")
table("business.daily_publications", "study_hk:s! local_date:d! input_manifest_hash:s! rule_version:s! code_version:s! status:s! published_at:t", "study_hk, local_date")
table("business.daily_evaluations", "evaluation_id:s! mart_evaluation_id:s! implementation_hash:s! input_manifest_hash:s! committed_at:t!", "evaluation_id")
table("business.daily_readiness_outbox", "evaluation_id:s! study_hk:s! local_date:d! input_manifest_hash:s! rule_version:s! code_version:s! validated_at:t! coverage_json:j! warning_counts_json:j!", "evaluation_id, study_hk, local_date")

table("marts.dim_participant", "participant_sk:s! participant_hk:s! participant_id:s! business_namespace:s! height_cm:n gender:s chronotype:s profile_version_id:s! known_from_at:t! known_to_at:t is_current:b!", "participant_sk", unique=("participant_hk, profile_version_id",))
table("marts.dim_device", "device_sk:s! device_hk:s! device_id:s! firmware_version:s firmware_provenance:s calibration_date:d device_version_id:s! known_from_at:t! known_to_at:t is_current:b!", "device_sk", unique=("device_hk, device_version_id",))
table("marts.dim_study", "study_sk:s! study_hk:s! study_code:s! study_timezone:s! study_start:d! study_end_exclusive:d!", "study_sk", unique=("study_hk",))
table("marts.dim_date", "date_sk:i! calendar_date:d! iso_week_start:d! iso_week_number:h! iso_week_year:h! day_of_week:h!", "date_sk", unique=("calendar_date",))
FACT = "study_hk:s! participant_hk:s! study_sk:s! participant_sk:s! device_sk:s date_sk:i! event_at_utc:t!"
table("marts.fact_steps_minute", FACT + " steps_per_minute:n! " + LINEAGE, "study_hk, participant_hk, event_at_utc", checks=("steps_per_minute >= 0",))
table("marts.fact_heart_rate_minute", FACT + " heart_rate_bpm:h! " + LINEAGE, "study_hk, participant_hk, event_at_utc", checks=("heart_rate_bpm BETWEEN 40 AND 200",))
SLEEP = "sleep_onset_utc:t sleep_end_utc:t reported_stage_sleep_min:n reported_stage_deep_min:n deep_sleep_pct_from_reported_stages:n efficiency_pct_reported:n restlessness:n"
table("marts.fact_sleep_session", "sleep_session_hk:s! study_hk:s! participant_hk:s! study_sk:s! participant_sk:s! date_sk:i! wake_date:d! " + SLEEP + " session_version_id:s! quality_status:s! rule_version:s!", "sleep_session_hk")
STAGES = "session_version_id:s! stage_position:i! sleep_session_hk:s! stage_type:s! start_at_utc:t end_at_utc:t duration_min_reported:n is_post_boundary_awake:b! warning_status:s"
table("marts.fact_sleep_stage", STAGES, "session_version_id, stage_position")
table("marts.fact_wellness_daily", "wellness_response_hk:s! study_hk:s! participant_hk:s! study_sk:s! participant_sk:s! date_sk:i! survey_date:d! " + SCORES + " response_version_id:s! quality_status:s! rule_version:s!", "wellness_response_hk", checks=("fatigue_score BETWEEN 1 AND 5", "stress_score BETWEEN 1 AND 5", "readiness_score BETWEEN 0 AND 10", "sleep_quality_score BETWEEN 1 AND 5"))
# dbt will populate the one-participant-row Gold contract after resolving assignments.
table("marts.gold_participant_devices", "participant_hk:s! participant_id:s! business_namespace:s! device_id:s wear_site:s firmware_version:s firmware_provenance:s calibration_date:d assignment_quality_status:s!", "participant_hk")

DAILY = "expected_minutes:i accepted_steps_minutes:i! accepted_heart_rate_minutes:i! observed_steps_total:n heart_rate_sum:i heart_rate_mean:n " + SLEEP + " " + SCORES + " has_steps:b! has_heart_rate:b! has_sleep:b! has_wellness:b! warning_count:i! quality_status:s! input_manifest_hash:s! rule_version:s! code_version:s! calculated_at:t!"
table("marts.participant_day_current", "study_hk:s! participant_hk:s! local_date:d! " + DAILY, "study_hk, participant_hk, local_date", checks=("accepted_steps_minutes >= 0", "accepted_heart_rate_minutes >= 0", "expected_minutes IS NULL OR expected_minutes >= 0"))

table("publication_build.publications", "publication_id:s! study_hk:s! week_start:d! revision:i! event_period_start:t! event_period_end:t! scheduled_run_at:t correction_cutoff_at:t! consolidation_date:d! published_at:t status:s! input_manifest_hash:s! rule_version:s! code_version:s!", "publication_id", unique=("study_hk, week_start, revision",), checks=("revision >= 1", "status IN ('building', 'validated', 'published', 'failed')", "status <> 'published' OR published_at IS NOT NULL"))
table("publication_build.participant_snapshot", "publication_id:s! participant_hk:s! participant_id:s! business_namespace:s! study_hk:s! chronotype:s gender:s height_cm:n age_at_enrollment:h max_heart_rate:h profile_version_id:s enrollment_version_id:s device_id:s assignment_version_id:s quality_status:s!", "publication_id, participant_hk")
table("publication_build.participant_day", "publication_id:s! participant_hk:s! study_hk:s! local_date:d! " + DAILY, "publication_id, participant_hk, local_date")
WEEK = "study_hk:s! week_start:d! expected_days:i! expected_minutes:i accepted_steps_minutes:i! accepted_heart_rate_minutes:i! observed_steps_total:n heart_rate_sum:i heart_rate_mean:n accepted_sleep_sessions:i! sleep_minutes_sum:n sleep_minutes_mean:n deep_sleep_pct_sum:n deep_sleep_pct_count:i! deep_sleep_pct_mean:n efficiency_pct_sum:n efficiency_pct_count:i! efficiency_pct_mean:n accepted_wellness_responses:i! fatigue_sum:n fatigue_count:i! fatigue_mean:n stress_sum:n stress_count:i! stress_mean:n readiness_sum:n readiness_count:i! readiness_mean:n sleep_quality_sum:n sleep_quality_count:i! sleep_quality_mean:n warning_count:i! partial_week:b! quality_status:s!"
table("publication_build.participant_week", "publication_id:s! participant_hk:s! " + WEEK, "publication_id, participant_hk")
table("publication_build.sleep_stages", "publication_id:s! " + STAGES, "publication_id, session_version_id, stage_position")
table("publication_build.sleep_stage_exclusions", "publication_id:s! session_version_id:s! stage_position:i! reason_code:s! issue_id:s! resolution_id:s!", "publication_id, session_version_id, stage_position")
table("publication_build.requests", "request_id:s! publication_id:s! scope_json:j!", "request_id")
table("publication_build.inputs", "publication_id:s! input_kind:s! version_id:s! source_relation:s! business_key_hash:s! payload_hash:s!", "publication_id, input_kind, version_id")
table("publication_build.readiness", "publication_id:s! local_date:d! revision:i! input_manifest_hash:s! rule_version:s! code_version:s! coverage_json:j! warning_counts_json:j!", "publication_id, local_date")
table("publication_build.sleep_stage_candidates", STAGES.replace("session_version_id:s!", "publication_id:s! session_version_id:s!"), "publication_id, session_version_id, stage_position")


def column_sql(spec, dialect):
    name, kind = spec.split(":")
    required = kind.endswith("!")
    return f"{name} {TYPES[dialect][kind.rstrip('!')]}" + (" NOT NULL" if required else "")


def view_sql(name, query, dialect):
    # dbt owns staging after the first build. Repeated bootstrap must preserve it.
    if name.startswith("staging."):
        secure = "SECURE " if dialect == "snowflake" else ""
        return f"CREATE {secure}VIEW IF NOT EXISTS {name} AS\n{query};\n"
    if dialect == "snowflake":
        return f"CREATE OR REPLACE SECURE VIEW {name} COPY GRANTS AS\n{query};\n"
    return f"CREATE OR REPLACE VIEW {name} AS\n{query};\n"


def extract(expression, field, kind, dialect):
    if dialect == "duckdb":
        value = f"json_extract_string({expression}, '$.{field}')"
        return value if kind == "s" else f"TRY_CAST({value} AS {TYPES[dialect][kind]})"
    value = f"{expression}:{field}::VARCHAR"
    functions = {"i": "TRY_TO_NUMBER", "h": "TRY_TO_NUMBER", "n": "TRY_TO_DECIMAL", "d": "TRY_TO_DATE", "t": "TRY_TO_TIMESTAMP_TZ"}
    if kind == "s":
        return value
    return f"{functions[kind]}({value}" + (", 18, 6)" if kind == "n" else ")")


def render(dialect):
    schemas = ["raw", "staging", "vault", "business", "marts", "publication_build", "analytics", "biostatistics"]
    header = "-- Generated by scripts/generate_ddl.py. Edit the generator, not this file.\n-- Foundation only: transformations and data-quality gates remain pipeline responsibilities.\n"
    if dialect == "snowflake":
        header += "-- Requires a deployment administrator. No remote execution has been performed.\nUSE ROLE SYSADMIN;\nCREATE DATABASE IF NOT EXISTS HEALTH_DATA_PROJECT;\nUSE DATABASE HEALTH_DATA_PROJECT;\nALTER SESSION SET TIMEZONE = 'UTC';\n"
    else:
        header += "SET TimeZone = 'UTC';\nBEGIN TRANSACTION;\n"
    statements = [header]
    for schema in schemas:
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema}" + (" WITH MANAGED ACCESS" if dialect == "snowflake" else "") + ";\n")
    for name, columns, pk, unique, checks in TABLES:
        definitions = [column_sql(c, dialect) for c in columns]
        definitions.append(f"PRIMARY KEY ({pk})")
        definitions.extend(f"UNIQUE ({key})" for key in unique)
        # Keep cross-engine quality requirements in the pipeline; avoid assuming
        # availability/enforcement of CHECK on an unverified Snowflake account.
        if dialect == "duckdb":
            definitions.extend(f"CHECK ({check})" for check in checks)
        statements.append(f"CREATE TABLE IF NOT EXISTS {name} (\n    " + ",\n    ".join(definitions) + "\n);\n")

    statements.append(render_catalogs(dialect, standalone=False))
    fields = {
        "participants": "participant_id:s device_id:s age:h height_cm:n gender:s chronotype:s max_heart_rate:h",
        "device_metadata": "participant_id:s device_id:s wear_site:s calibration_date:d firmware_version:s device_label:s",
        "steps": "participant_id:s timestamp:s steps:i _unit:s",
        "heart_rate": "participant_id:s timestamp:s heart_rate_bpm:i",
        "sleep_sessions": "participant_id:s date:d sleep_onset:s sleep_end:s efficiency_pct:n restlessness:n",
        "wellness": "participant_id:s date:d " + SCORES,
    }
    aliases = {"age": "age_at_enrollment", "firmware_version": "firmware_version_reported", "timestamp": "timestamp_original", "steps": "steps_reported", "_unit": "unit_reported", "sleep_onset": "sleep_onset_original", "sleep_end": "sleep_end_original", "efficiency_pct": "efficiency_pct_reported"}
    for name, columns in fields.items():
        source = {"sleep_sessions": "sleep", "wellness": "wellness_survey"}.get(name, name)
        selected = ["r.record_id", "b.batch_id", "b.delivery_sequence", "b.source_system AS record_source", "r.payload_json", "r.ingested_at"]
        selected.append("COALESCE(" + extract("r.payload_json", "_op", "s", dialect) + ", 'upsert') AS operation")
        for spec in columns.split():
            field, kind = spec.split(":")
            column = aliases.get(field, field)
            if field == "date":
                column = "wake_date" if name == "sleep_sessions" else "survey_date"
            selected.append(extract("r.payload_json", field, kind, dialect) + " AS " + column)
        query = "SELECT " + ",\n       ".join(selected) + f"\nFROM raw.source_records r JOIN raw.batch_records b ON b.record_id = r.record_id\nWHERE r.source_name = '{source}'"
        statements.append(view_sql("staging.stg_" + name, query, dialect))

    stage_columns = ["s.record_id AS session_record_id", "s.batch_id", "s.delivery_sequence", "s.record_source", "s.participant_id", "s.wake_date"]
    if dialect == "duckdb":
        stage_columns.append("TRY_CAST(x.key AS BIGINT) AS stage_position")
        stage_value = "x.value"
        stage_from = "FROM staging.stg_sleep_sessions s, LATERAL json_each(s.payload_json, '$.stages') x"
    else:
        stage_columns.append("x.index AS stage_position")
        stage_value = "x.value"
        stage_from = "FROM staging.stg_sleep_sessions s, LATERAL FLATTEN(INPUT => s.payload_json:stages) x"
    for field, kind, alias in [("stage", "s", "stage_type"), ("start_time", "s", "start_original"), ("end_time", "s", "end_original"), ("duration_min", "n", "duration_min_reported")]:
        stage_columns.append(extract(stage_value, field, kind, dialect) + " AS " + alias)
    statements.append(view_sql("staging.stg_sleep_stages", "SELECT " + ",\n       ".join(stage_columns) + "\n" + stage_from, dialect))

    for name in ("dim_participant", "dim_device", "dim_study", "dim_date", "gold_participant_devices"):
        statements.append(view_sql("analytics." + name, f"SELECT * FROM marts.{name}", dialect))
    statements.append(view_sql("analytics.participant_day", "SELECT d.* FROM marts.participant_day_current d\nJOIN business.daily_publications p ON p.study_hk = d.study_hk AND p.local_date = d.local_date\nAND p.input_manifest_hash = d.input_manifest_hash AND p.rule_version = d.rule_version AND p.code_version = d.code_version\nWHERE p.status = 'published' AND p.published_at IS NOT NULL\nAND d.quality_status IN ('accepted', 'accepted_with_warning')", dialect))
    statements.append(view_sql("biostatistics.publications", "SELECT * FROM publication_build.publications WHERE status = 'published' AND published_at IS NOT NULL", dialect))
    for name in ("participant_snapshot", "participant_day", "participant_week", "sleep_stages", "sleep_stage_exclusions"):
        statements.append(view_sql("biostatistics." + name, f"SELECT d.* FROM publication_build.{name} d\nJOIN biostatistics.publications p ON p.publication_id = d.publication_id", dialect))
    statements.append(view_sql("biostatistics.latest_participant_week", "SELECT d.* FROM biostatistics.participant_week d\nJOIN (SELECT study_hk, week_start, MAX(revision) AS revision FROM biostatistics.publications\n      GROUP BY study_hk, week_start) latest\nON latest.study_hk = d.study_hk AND latest.week_start = d.week_start\nJOIN biostatistics.publications p ON p.publication_id = d.publication_id AND p.revision = latest.revision", dialect))
    if dialect == "duckdb":
        statements.append("COMMIT;\n")
    return "\n".join(statements)


def render_catalogs(dialect, standalone=True):
    """Create and seed equivalence catalogs without modifying existing ledger rows."""
    statements = []
    if standalone:
        statements.append('-- Generated by scripts/generate_ddl.py. Reference catalogs only.')
        statements.append('BEGIN TRANSACTION;' if dialect == 'duckdb' else
                          'USE ROLE SYSADMIN;\nUSE DATABASE HEALTH_DATA_PROJECT;')
        for catalog in QUALITY_CATALOGS:
            statements.append(f'CREATE TABLE IF NOT EXISTS business.{catalog} (\n'
                              '    code VARCHAR NOT NULL PRIMARY KEY,\n'
                              '    display_name VARCHAR NOT NULL,\n'
                              '    description VARCHAR NOT NULL\n);')
    for catalog, rows in QUALITY_CATALOGS.items():
        for code, label, description in rows:
            values = ', '.join("'" + value.replace("'", "''") + "'" for value in (code, label, description))
            statements.append(f'INSERT INTO business.{catalog} (code, display_name, description)\n'
                              f'SELECT {values}\nWHERE NOT EXISTS '
                              f"(SELECT 1 FROM business.{catalog} WHERE code = '{code}');")
    if standalone and dialect == 'duckdb':
        statements.append('COMMIT;')
    return '\n\n'.join(statements) + '\n'


def render_quality_migration():
    """Rebuild only the quality ledger tables with enforced domain checks."""
    statements = ["-- Generated by scripts/generate_ddl.py.\n-- Run with all warehouse tasks idle. Invalid existing values abort the transaction.\nBEGIN TRANSACTION;"]
    for name, columns, pk, _, checks in TABLES:
        if name not in ('business.quality_issues', 'business.quality_resolutions'):
            continue
        replacement = name + '_domain_migration'
        definitions = [column_sql(c, 'duckdb') for c in columns]
        definitions.append(f'PRIMARY KEY ({pk})')
        definitions.extend(f'CHECK ({check})' for check in checks)
        fields = ', '.join(c.split(':')[0] for c in columns)
        statements.extend([
            f'CREATE TABLE {replacement} (\n    ' + ',\n    '.join(definitions) + '\n);',
            f'INSERT INTO {replacement} ({fields}) SELECT {fields} FROM {name};',
            f'DROP TABLE {name};',
            f'ALTER TABLE {replacement} RENAME TO {name.split(".")[1]};',
        ])
    return '\n\n'.join(statements + ['COMMIT;\n'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if generated SQL differs from committed files")
    parser.add_argument("--output-root", type=Path, default=ROOT / "infra" / "ddl")
    args = parser.parse_args()
    for dialect in TYPES:
        path = args.output_root / dialect / "001_foundation.sql"
        content = render(dialect)
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                raise SystemExit(f"Generated DDL is stale: {path}")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")
        print(f"{dialect}: {len(TABLES)} foundation tables")
    path = args.output_root / 'duckdb' / '002_quality_domains.sql'
    content = render_quality_migration()
    if args.check:
        if not path.exists() or path.read_text(encoding='utf-8') != content:
            raise SystemExit(f'Generated DDL is stale: {path}')
    else:
        path.write_text(content, encoding='utf-8', newline='\n')
    for dialect in TYPES:
        path = args.output_root / dialect / '003_quality_catalogs.sql'
        content = render_catalogs(dialect)
        if args.check:
            if not path.exists() or path.read_text(encoding='utf-8') != content:
                raise SystemExit(f'Generated DDL is stale: {path}')
        else:
            path.write_text(content, encoding='utf-8', newline='\n')


if __name__ == "__main__":
    main()
