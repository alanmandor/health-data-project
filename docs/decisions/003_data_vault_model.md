# ADR 003: Data Vault Integration Model

Status: Data Vault selected; entity design accepted as the implementation starting point.

## Entities and Grain

| Entity | Proposed Structures | Business Key / Grain |
| --- | --- | --- |
| Participant | hub_participant, sat_participant_profile | Source namespace + participant_id |
| Device | hub_device, sat_device_metadata | Source namespace + device_id |
| Assignment | link_participant_device, assignment state/effectivity satellite | Participant-device relationship; effective dates only when supplied |
| Study membership | hub_study, link_participant_study, enrollment satellite | Study and participant; requires an explicit study identifier |
| Steps | Participant-attached observation satellite | Participant + UTC timestamp + accepted observation version |
| Heart rate | Separate participant-attached observation satellite | Participant + UTC timestamp + accepted observation version |
| Sleep | hub_sleep_session, link_participant_sleep_session, session and stage satellites | Study + participant + wake-up date under the current one-session-per-day contract |
| Wellness | hub_wellness_response, link_participant_wellness_response, response satellite | Study + participant + survey date under the current one-response-per-day contract |

Do not create a hub for every minute reading. Minute observation satellites require a documented dependent discriminator (UTC timestamp) and a collision-safe version identity; they are not conventional single-state participant profile satellites. Stage records use a session version and array position so corrections preserve each complete historical stage set.

Sleep and survey event keys must expand to producer session_id/response_id if multiple events per day become valid. Do not infer historical device identity from the current assignment: wearable records lack device_id. Device attribution after reassignment requires effective assignment intervals and an unambiguous source contract.

Age is age at enrollment, not a continuously current age. Place it in enrollment context when multiple studies are introduced. Do not silently reinterpret calibration metadata as device-independent participant attributes.

## History and Lineage

Use deterministic hash keys with an explicitly versioned canonicalization specification, source namespace, defined null handling, and collision-safe field encoding. Store record_source, batch_id, stable version identity, load time, and payload hash. Preserve event time separately. Effective time is unknown unless supplied or explicitly derived under a documented rule.

Compare an accepted change with the preceding accepted state for its business key/discriminator. A -> B -> A creates three versions; deduplication against every historical payload hash would incorrectly lose the final transition. A repeated batch/version creates no additional business history.

Raw storage retains original records. The vault retains structurally valid source observations and lineage, with unresolved conflicts distinguishable from accepted state; business rules and current projections determine analytical eligibility. Conflicting values within one batch do not establish an authoritative sequence of changes.

Unknown participant events remain quarantined. If observed identifiers are retained in a hub, they do not establish master authorization or enrollment. Validated analytical eligibility depends on the participant master.

## Implementation Boundaries

Describe the implementation as Data Vault 2.0-aligned. Final physical column definitions, hash serialization, satellite loading macros, and any automation package require compatibility and replay checks before implementation. Study/enrollment structures require an explicit study identifier and must not fabricate source enrollment dates. Kimball marts remain the consumption layer; preserve gold_participant_devices with one row per participant.
