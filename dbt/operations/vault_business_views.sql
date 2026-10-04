CREATE OR REPLACE VIEW business.vault_decisions_current AS
SELECT * FROM business.version_decisions
QUALIFY row_number() OVER (PARTITION BY source_relation, version_id ORDER BY decided_at DESC, decision_id DESC)=1;

-- Structurally valid unknown identities are observed hubs, not enrollments.
-- The current quality registry still controls analytical authorization.
CREATE OR REPLACE VIEW business.vault_accepted_versions AS
SELECT v.*, d.business_key_hash, d.normalized_payload_hash, d.rule_version
FROM business.vault_versions v
JOIN business.vault_decisions_current d USING (source_relation, version_id)
WHERE d.decision='accepted' AND coalesce(d.reason_code, '') <> 'identical_duplicate'
AND EXISTS (
    SELECT 1 FROM vault.hub_participant h
    JOIN business.quality_participants_current p
        ON p.record_source=h.business_namespace AND p.participant_id=h.participant_id
    WHERE h.participant_hk=v.participant_hk
);

CREATE OR REPLACE VIEW business.vault_accepted_changes AS
WITH previous AS (
    SELECT *, lag(payload_hash) OVER (
        PARTITION BY source_relation, business_key_hash ORDER BY delivery_sequence, version_id) AS previous_hash
    FROM business.vault_accepted_versions
)
SELECT * EXCLUDE (previous_hash) FROM previous WHERE payload_hash IS DISTINCT FROM previous_hash;

CREATE OR REPLACE VIEW business.vault_current_versions AS
WITH latest AS (
    SELECT * FROM business.vault_accepted_versions
    QUALIFY row_number() OVER (
        PARTITION BY source_relation, business_key_hash ORDER BY delivery_sequence DESC, version_id DESC)=1
)
SELECT * FROM latest WHERE operation='upsert';
