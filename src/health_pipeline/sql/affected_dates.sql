-- Hash accepted values and contextual warnings; batch identities are not changes.
CREATE TEMP TABLE impact_observation_days AS
SELECT q.record_source, q.participant_id,
       CASE WHEN q.source_name IN ('steps', 'heart_rate') THEN q.local_date
            ELSE q.observation_date END AS local_date,
       sha256(string_agg(sha256(to_json(list_value(q.source_name, q.business_key_hash,
           q.normalized_payload_hash, q.quality_status, to_json(list_sort(q.warning_codes))::VARCHAR))),
           '' ORDER BY q.source_name, q.business_key_hash)) AS observation_hash
FROM business.quality_current q
WHERE q.source_name IN ('steps', 'heart_rate', 'sleep', 'wellness_survey')
GROUP BY q.record_source, q.participant_id, 3;

CREATE TEMP TABLE impact_references AS
SELECT p.record_source, p.participant_id, h.participant_hk, p.normalized_payload_hash AS profile_hash,
       m.normalized_payload_hash AS metadata_hash,
       count(*) OVER (PARTITION BY p.record_source, p.device_id) AS device_owners
FROM business.quality_participants_current p
JOIN vault.hub_participant h ON h.business_namespace=p.record_source AND h.participant_id=p.participant_id
LEFT JOIN business.quality_current m ON m.source_name='device_metadata'
    AND m.record_source=p.record_source AND m.participant_id=p.participant_id AND m.device_id=p.device_id;

-- Include configured coverage days even when no accepted observations exist.
CREATE TEMP TABLE impact_new_state AS
WITH days AS (
    SELECT r.record_source, r.participant_id, r.participant_hk, d.day::DATE AS local_date
    FROM impact_references r CROSS JOIN vault.sat_study s,
         LATERAL generate_series(s.study_start, s.study_end_exclusive-INTERVAL '1 day', INTERVAL '1 day') d(day)
    UNION
    SELECT r.record_source, r.participant_id, r.participant_hk, o.local_date
    FROM impact_references r JOIN impact_observation_days o USING (record_source, participant_id)
)
SELECT c.study_hk, d.participant_hk, d.local_date,
       sha256(to_json(list_value('participant_day_state', r.profile_hash, r.metadata_hash,
           o.observation_hash, r.device_owners::VARCHAR, c.quality_code, c.implementation_hash, c.study_timezone))) AS state_hash
FROM days d JOIN impact_references r USING (record_source, participant_id, participant_hk)
LEFT JOIN impact_observation_days o USING (record_source, participant_id, local_date)
CROSS JOIN impact_configuration c;

CREATE TEMP TABLE impact_changes AS
SELECT coalesce(n.study_hk, p.study_hk) AS study_hk,
       coalesce(n.participant_hk, p.participant_hk) AS participant_hk,
       coalesce(n.local_date, p.local_date) AS local_date,
       p.state_hash AS previous_hash, n.state_hash AS next_hash,
       CASE WHEN p.state_hash IS NULL THEN 'initial_state'
            WHEN n.state_hash IS NULL THEN 'current_state_removed'
            ELSE 'current_state_changed' END AS reason_code
FROM impact_new_state n FULL OUTER JOIN business.participant_date_state p
    USING (study_hk, participant_hk, local_date)
WHERE p.state_hash IS DISTINCT FROM n.state_hash;
