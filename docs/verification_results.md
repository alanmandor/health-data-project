# Local Verification Results

The October 4, 2026 local exercise verification runs in Docker with Airflow 3.1.7, PostgreSQL 16.13, dbt Core 1.11.15, dbt-duckdb 1.11.0, and DuckDB 1.5.6. Verification uses the pinned container environments; host Python is not required. Snowflake DDL and roles have not been executed in a cloud account.

## Baseline Output

The supplied baseline has 4,032,814 raw records in 153 files. Current analytical facts contain 2,005,573 steps minutes, 2,003,983 heart-rate minutes, 1,400 sleep sessions, 7,000 provisional stages, and 1,400 accepted wellness responses. The authoritative cohort has 50 participants; the 14 P999 surveys remain recoverable pending references. Fifteen firmware values retain device-label provenance.

Daily analytics publishes 1,400 participant/date rows across the 28 explicitly declared study dates. The five Monday-based weekly publications contain 250 frozen cohort snapshots, 1,400 frozen daily rows, and 250 participant/week rows. First and last weeks cover four and three study days respectively. Frozen stage output retains 5,603 stages and records 1,397 terminal awake exclusions; the full 7,000 provisional stages remain unchanged.

P001's January 21 output retains 327 reported sleep minutes and 100% reported efficiency. Its 55-minute terminal awake interval is excluded only from the consolidated stages, with a publication-linked resolution. Other semantic warnings remain visible.

## Verification Coverage

- Thirty Python tests cover calendar/manifest contracts, raw capture/load/archive recovery, real dbt corrections/deletes/reference recovery, Vault rollback and history, dimensional updates, daily publication/retry, and frozen weekly publication/revisions.
- Six foundation tests verify all 57 tables, repeatable bootstrap, valid facts versus invalid raw values, publication gating, immutable revision visibility, staging casts/arrays, and SQLite control preservation.
- Daily and weekly DAGs each run twice using isolated temporary storage. Verification checks four source files archived once, one frozen publication, readiness fingerprints, and a terminal awake exclusion without deleting provisional stages.
- Independent baseline checks reconcile raw/quality counts, Vault lineage, issue/resolution integrity, affected-date consumption, dimensional facts, daily totals, weekly totals, retained/excluded stages, selected version counts, and exported gzip manifest checksums.
- An explicit five-week replay reuses all existing publication identities and cutoffs without creating new revisions. Generated DuckDB and Snowflake DDL match their shared source definition, and both Airflow DAGs import successfully.

Run verification only while other tasks accessing the real warehouse are idle. The isolated unit suite uses separate temporary databases; baseline verifiers use read-only connections. Do not overlap a read-only verifier with a DuckDB writer.

## Practical Limits

Both DAGs are manual until the operational daily trigger, availability timeout, and expected producer inventory are configured. The tentative weekly production time is Monday 03:00 America/Bogota. The exercise does not supply live daily manifests or a producer SLA.

The loader supports additional participants and corrections without a fixed 50-user cap. This deployment registers one study per warehouse. Daily aggregates currently recompute the compact daily result rather than update only affected date groups. Large source files need streaming or an agreed chunk contract. DuckDB work is serialized and spills temporary state to disk under a managed memory limit; local frozen copies do not simulate Snowflake Zero-Copy Clone.

These are explicit implementation boundaries, not claims of production deployment or clinical correctness. The [Docker quick start](docker_quickstart.md) and [weekly guide](weekly_biostatistics.md) describe reproducible local execution.
