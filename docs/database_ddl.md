# Database Foundation and Consumer Contracts

## Exercise Requirements

The assessment brief, page 2, requires daily freshness for clinical analytics and weekly batches with a different schema for biostatistics. It leaves schemas, layers, and storage engines to the implementation. It does not prescribe biostatistics tables or columns. It requires chronotype comparisons, and page 4 requires a Gold table named gold_participant_devices with one row per participant. These contracts are project decisions, not a supplied biostatistics specification. Source reviewed: brief_ic4.pdf, originally supplied with the exercise. The repository data dictionary defines source fields, not a consumer schema.

## Foundation Files

- [DuckDB foundation](../infra/ddl/duckdb/001_foundation.sql): 44 tables and staging/consumer views across eight schemas.
- [Snowflake foundation](../infra/ddl/snowflake/001_foundation.sql): matching logical contracts, native types, managed-access schemas, and secure views.
- [Snowflake team roles](../infra/ddl/snowflake/002_reader_roles.sql): published-data read access, writable team workspaces, and dedicated query warehouses. The filename is retained for compatibility.
- [SQLite controls](../infra/ddl/sqlite/001_control_registry.sql): extends the existing delivery registry without dropping records.

These scripts create empty objects. Ingestion, CDC selection, timestamp normalization, quality gates, and publication remain pipeline work. Business current-state views will be implemented with dbt. Staging preserves payloads and timestamp strings; safe-cast failures become NULL and require explicit parsing diagnostics and quarantine decisions.

scripts/generate_ddl.py is the source for both foundation files. Regenerate both dialects together. CREATE TABLE IF NOT EXISTS supports repeatable setup but does not migrate columns. Later structural changes need migrations. DuckDB bootstrap is transactional; Snowflake DDL commits independently and can leave partial setup after failure.

DuckDB enforces declared primary/unique keys and selected CHECK constraints. Snowflake standard-table primary/unique keys are informational; this foundation omits Snowflake CHECK constraints. Pipeline tests must enforce uniqueness, relationships, ranges, and publication checks on both engines. Foreign keys are omitted to support transactional delete/insert projections. Read access alone does not guarantee quality.

## Consumer Contracts

marts.gold_participant_devices is a physical Gold table, exposed through analytics.gold_participant_devices. It contains one row per authorized participant and flags ambiguous assignments.

analytics.participant_day exposes accepted metrics only with a published business.daily_publications record matching the study/date, input hash, rule version, and code version. Recalculation and publication must commit together to retain complete validated daily output. Public dimension/Gold views rely on transactional validated updates and have no separate publication filter.

Weekly candidates are internal tables in publication_build. Public views in biostatistics expose published revisions only:

| View | Contract |
| --- | --- |
| publications | Study/week scope, revision, correction cutoff, consolidation date, input hash, rules/code versions, publication timestamp |
| participant_snapshot | Frozen profile, chronotype, enrollment context, and assignment-version references |
| participant_day | Frozen daily metrics, coverage, and warnings |
| participant_week | Weekly observed sums, eligible denominators, means, coverage, partial-week flag |
| sleep_stages | Retained consolidated stage records with reported durations |
| sleep_stage_exclusions | Audit references for excluded discordant post-boundary awake records |
| latest_participant_week | Highest published revision per study/week; drafts cannot replace it |

Join snapshots and metrics on publication_id and participant_hk. Earlier revisions remain queryable with their original cohort attributes. The pipeline must freeze selected input versions, prohibit edits to published content, verify completeness and relationships, and publish only after validation. Views hide drafts but do not independently enforce completeness or administrator immutability.

The correction window closes when Monday consolidation freezes inputs after validated Sunday processing. Monday 03:00 America/Bogota is the tentative trigger, subject to source availability. Event-week boundaries are Monday midnight to next Monday midnight, exclusive. See [publication schedule](publication_schedule.md) and [time policy](time_policy.md).

## Snowflake Access and Storage

Deploy foundation SQL first, then reader-role SQL, with the required administrator privileges. Review database/warehouse names before execution. Snowflake execution has not been validated because no account is available.

| Role | Data access | Compute access |
| --- | --- | --- |
| ANALYTICS_ROLE | SELECT on ANALYTICS views; CREATE TABLE/VIEW and table DML in ANALYTICS_WORKSPACE | HEALTH_ANALYTICS_WH USAGE |
| BIOSTATISTICS_ROLE | SELECT on BIOSTATISTICS views; CREATE TABLE/VIEW and table DML in BIOSTATISTICS_WORKSPACE | HEALTH_BIOSTATISTICS_WH USAGE |

The published ANALYTICS and BIOSTATISTICS schemas remain owned by Data Engineering's deployment role and read-only for consumers. Each team has its own writable workspace, avoiding name collisions with supplied views and accidental replacement of published objects. The script grants USAGE and CREATE TABLE/VIEW in that workspace, table SELECT/INSERT/UPDATE/DELETE/TRUNCATE on existing and future workspace tables, and SELECT on workspace views. Objects created using the team role are owned by that role, allowing ALTER, DROP, and CREATE OR REPLACE on its objects. Team-created temporary, transient, or permanent tables are supported. No CREATE or DML is granted on published or internal schemas. Future grants on published schemas still cover views only. Published view replacements use COPY GRANTS.

Use the team role when creating shared team objects; individual roles may otherwise own objects that teammates cannot alter. Both workspaces use managed access, with centralized grant administration. This does not prevent owners from changing their own data or object definitions. Each team is isolated from the other workspace. Broader account permissions and existing user roles are unchanged.

```sql
USE ROLE ANALYTICS_ROLE;
USE WAREHOUSE HEALTH_ANALYTICS_WH;
CREATE TABLE HEALTH_DATA_PROJECT.ANALYTICS_WORKSPACE.participant_steps AS
SELECT participant_hk, SUM(observed_steps_total) AS observed_steps_total
FROM HEALTH_DATA_PROJECT.ANALYTICS.participant_day
GROUP BY participant_hk;

USE ROLE BIOSTATISTICS_ROLE;
USE WAREHOUSE HEALTH_BIOSTATISTICS_WH;
CREATE VIEW HEALTH_DATA_PROJECT.BIOSTATISTICS_WORKSPACE.chronotype_cohort AS
SELECT w.publication_id, s.chronotype,
       AVG(w.observed_steps_total) AS mean_observed_weekly_steps
FROM HEALTH_DATA_PROJECT.BIOSTATISTICS.latest_participant_week w
JOIN HEALTH_DATA_PROJECT.BIOSTATISTICS.participant_snapshot s
  ON s.publication_id = w.publication_id AND s.participant_hk = w.participant_hk
GROUP BY w.publication_id, s.chronotype;
```

The cohort example is exploratory and does not adjust for partial coverage. A derived table is a snapshot requiring an explicit refresh; a normal view reflects the current published source selection.

Managed access centralizes grants. User assignments are explicit commented examples; the script creates no users and revokes no existing grants. Audit existing roles, inheritance, PUBLIC, and secondary roles before assigning users. Test with secondary roles disabled. Granting reader roles to SYSADMIN makes administrators inherit reader privileges, not the reverse.

SELECT and normal derived views do not rewrite source-table micro-partitions. Derived tables and their updates consume storage and compute in the workspaces, without rewriting Data Engineering tables. Pipeline ingestion, delete/insert updates, and publications can still create source partitions. The foundation defines no clustering keys; assess measured pruning and scale before adding automatic clustering. Readers cannot alter source clustering keys or source tables under these grants. Workspace owners can choose clustering for their own tables and incur associated maintenance costs. Dedicated warehouses isolate compute, not storage, and auto-suspend is not a hard spending cap. No new account-wide resource restrictions are introduced. Zero-Copy Clone is independent and is not simulated by DuckDB. References: [Snowflake access control](https://docs.snowflake.com/en/user-guide/security-access-control-considerations), [storage considerations](https://docs.snowflake.com/en/user-guide/tables-storage-considerations), [automatic clustering](https://docs.snowflake.com/en/user-guide/tables-auto-reclustering).

DuckDB does not reproduce Snowflake role enforcement. These consumer workspaces are Snowflake deployment objects; local DuckDB access remains controlled by the application and filesystem.

## Verification

From the repository root with Docker running:

```powershell
docker compose cp infra/ddl airflow-scheduler:/tmp/reviewed-ddl
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/generate_ddl.py --check --output-root /tmp/reviewed-ddl
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_ddl.py --ddl-root /tmp/reviewed-ddl
```

Six tests use temporary in-memory databases to verify all 44 tables, repeatable setup with records, invalid raw values versus validated facts, draft filtering, latest published revisions, frozen cohort attributes, safe casts, sleep array positions, and SQLite preservation. They do not initialize or modify the persistent warehouse.

Execute local foundation SQL against the chosen DuckDB warehouse with the dbt environment's connection when initialization is needed. Orchestrated access must use the shared duckdb_access pool. Snowflake syntax, actual grants, queries, and denied writes require validation on a real account.
