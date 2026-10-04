# Project Scope, Limits, and Trade-offs

## Completed Local Scope

The local implementation processes explicit delta deliveries through immutable raw
storage, dbt staging and quality, Data Vault history, dimensional marts, daily
analytics, and frozen weekly biostatistics publications. Airflow orchestrates the
complete workflows with retries and readiness gates. Docker provides the runtime;
DuckDB stores analytical data, PostgreSQL stores Airflow metadata, and SQLite stores
delivery and readiness controls.

The foundation contains 60 tables, including three populated quality equivalence
catalogs. [Verification results](verification_results.md) document the supplied
baseline, the 30 pipeline tests, nine foundation tests, repeated DAG execution,
and independent reconciliations. The latest catalog changes received targeted
schema and equivalence checks rather than another full pipeline replay.

The following boundaries are deliberate scope decisions or unverified deployment
capabilities. They do not establish production readiness or clinical validity.

## Operational and Technical Boundaries

| Boundary | Reason and practical effect | What an extension requires |
| --- | --- | --- |
| Manual Airflow triggers | Both DAGs use `schedule=None`. The exercise provides no live delivery SLA. A manual trigger starts the complete automated task chain, including retries; no automatic calendar trigger is enabled. | Agree on the daily trigger, expected producer inventory, and finite availability timeout before activating schedules. The tentative weekly trigger is Monday at 03:00 in America/Bogota, conditional on validated required deliveries. |
| Explicit delivery contracts | Files are treated as deltas, with explicit identity, scope, corrections, and deletes. Missing rows do not imply deletion. This interpretation enables growth but is a project assumption rather than a supplied producer guarantee. | Confirm the contract with each producer, including complete-record corrections, deletion semantics, unit conventions, availability, and late deliveries. |
| DuckDB concurrency and memory | The single-slot `duckdb_access` pool serializes orchestrated warehouse work. Managed memory limits and disk spill support the supplied baseline. Direct CLI access must be coordinated separately; the pool does not lock out every external process. | Measure larger workloads and concurrent consumption, tune resources and spill storage, or move to a warehouse with the required concurrency. No throughput SLA or load-test result is claimed. |
| Partial incremental processing | Raw ingestion is incremental and idempotent. Quality staging reevaluates retained raw history to recover late references and corrections. Marts consume affected dates, while daily aggregates currently recompute the compact daily result. | Introduce scoped quality and daily reevaluation only with dependency tracking and tests for late references, corrections, deletes, and contextual alerts. An event-time watermark alone could miss these changes. |
| Large source files | The supplied files work locally, but general large-file streaming is outside this implementation. Warehouse spill does not by itself bound memory used while parsing a large JSON document. | Add streaming parsing or an agreed chunked delivery format, preserving whole-delivery completeness and retry identity. |
| One study per warehouse | Participant growth is supported without a 50-user cap. The deployment registers one study per warehouse, and historical enrollment or assignment dates are not invented when absent from the source. | Establish study and producer identity namespaces, effective-date contracts, and multi-study isolation before combining studies. |
| Snowflake deployment | Foundation DDL, consumer roles/workspaces, and quality catalog seeds are supplied. They have not run in a cloud account. Current transformation SQL and macros use DuckDB functions; a Snowflake profile alone does not make the pipeline portable. | Adapt normalization, materialization, and orchestration SQL, then verify data reconciliation, actual grants, denied writes, and cost behavior in a real account. Snowflake domain and relationship validation must be implemented in the pipeline rather than assumed from the local constraints. |
| Frozen publications and storage | Local copies preserve selected inputs and published revisions. They consume storage and do not simulate Snowflake Zero-Copy Clone. A valid late correction produces a new revision without changing the old publication. | Define retention and archival policies, measure storage growth, and evaluate cloud cloning separately if adopted. Administrators still have physical write access; immutability is enforced by the publication workflow, not administrator-proof storage. |
| Dependency reproducibility | Top-level package and image versions are pinned, and Airflow uses version-specific constraints. Transitive dbt dependencies resolve during builds, so this is not a fully hashed offline dependency lock. | Produce a tested dependency lock and retain a versioned image or image digest for stricter build reproducibility. |
| Operational support | Quality issues and resolutions are persisted and queryable. No external notification destination, staffed review process, or tested backup/restore procedure is claimed. Docker volume persistence is not a backup. | Assign issue owners and escalation rules, configure notification destinations, and test coordinated restoration of the warehouse, registry, raw files, frozen exports, and Airflow metadata. |

## Data Interpretation Limits

Quality rules distinguish rejected values, pending references, conflicts, and
accepted measurements with warnings. These engineering decisions do not certify
clinical correctness. High step rates and constant heart-rate runs remain review
candidates where the source contract does not justify automatic rejection.

Missing observations remain missing. Coverage and eligible denominators are
published; absent measurements are not replaced by zeros. First and last study
weeks are partial, so comparisons must account for coverage. The example
chronotype query is descriptive and does not provide an inferential statistical
model, adjusted comparison, or clinical conclusion.

Reported sleep durations and efficiency are preserved while semantic discrepancies
remain visible. Internal awakenings followed by more sleep are valid. Weekly
consolidation may exclude only terminal post-boundary awake intervals satisfying
the documented rule, with publication-linked audit records. Raw, Vault, and
provisional stages remain intact. Closing such a boundary issue does not resolve
unrelated discrepancies or prove that the reported sleep values are correct.

The correction cutoff is the actual input-freeze instant after readiness checks,
not an unconditional Monday clock time. Delayed required data delays consolidation.
An exact publication retry reuses its frozen inputs; later accepted corrections
require a new publication revision.

## Delivery Closure

The README and linked guides explain how to build and execute the local exercise.
Before handing over repository access, publish the latest documentation, constraints,
and catalog changes and verify that the remote contains them. A separate startup
from a clean clone using fresh project volumes is recommended to validate the
handoff instructions; the existing environment verification does not establish
that this separate rehearsal has occurred.

Automatic scheduling and cloud deployment remain explicitly outside the completed
local delivery. These boundaries should accompany the exercise submission so that
evaluators can distinguish demonstrated behavior from future production work.
