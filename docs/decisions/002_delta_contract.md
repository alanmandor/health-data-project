# ADR 002: Delta Deliveries and Idempotent Updates

Status: accepted project contract; not guaranteed by the exercise's source specification.

## Delivery Contract

Treat the initial files as a bootstrap delta and subsequent deliveries as deltas containing new records or corrections. Each record contains the complete state of its observation, not a partial field patch. Missing records do not imply deletion. Deletion requires an explicit operation and business key. A key change requires deletion of the old key and insertion of the new key.

Each batch has a stable identifier, immutable file checksums, and a persisted delivery sequence. Under this project's explicit assumption, a later accepted batch supersedes an earlier version of the same key. Arrival order is not treated as evidence outside this contract. Future integration with a real producer must validate sequencing and revision semantics; until then, assign and persist sequence numbers on batch registration. Replays retain the original sequence and cannot overwrite newer accepted state.

## State Versus History

Use transactional delete/insert for current facts and recalculated aggregates: validate and deduplicate the delta, identify accepted keys, delete only those target keys, insert complete replacements, and commit. Rejected corrections must not delete valid prior values. Explicit deletions remove current contributions while retaining audit history.

Data Vault history is append-only at the logical level. Insert new accepted versions rather than deleting prior versions. Track source observation history separately from the accepted analytical state. Retries do not append the same version again or change business timestamps.

Manifest content identity is (source, object_key, SHA-256). Preserve file occurrences and original record positions. Maintain per-model processing state so publication failure does not require repeating source ingestion. Commit target writes before marking the work complete; retries after a checkpoint failure apply the same changes safely. Coordinate all database writers.

Incremental selection uses pending batches and persisted affected keys rather than event-time maxima. Corrections, authorized deletions, and resolved references update affected participant dates and weeks. Recalculate full groups from current facts, removing obsolete aggregates when appropriate.

## References and Growth

The participant master authorizes new participants. Fifty is a baseline expectation, not an identifier or schema limit. Unknown participants remain in raw and recoverable quarantine and do not enter validated metrics. Retry their records after master updates and propagate resolved changes to historical dates and publication revisions.

Expected coverage depends on study membership and the applicable period. Until effective enrollment dates are supplied, use the declared initial study window for baseline members and flag unknown future coverage scope rather than inventing dates.

## Acceptance Checks

Verify replay stability, recovery after writes but before checkpointing, old-batch replay after a newer correction, late historical data, new participants, reference resolution, conflicting duplicates, and explicit deletions. Compare current facts, business history, and aggregates; execution logs may differ.
