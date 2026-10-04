-- Current warnings include sequence-sensitive rules such as heart-rate flatlines.
CREATE TEMP TABLE issue_current_warnings AS
SELECT batch_id, record_id, unnest(warning_codes) AS rule_code
FROM business.quality_current WHERE len(warning_codes)>0;

CREATE TEMP TABLE issue_observed AS
WITH observations AS (
    SELECT batch_id, record_id, business_key_hash, delivery_sequence,
           unnest(warning_codes) AS rule_code, 'warning' AS severity
    FROM business.record_quality WHERE decision='accepted' AND operation='upsert'
    UNION ALL
    SELECT q.batch_id, q.record_id, q.business_key_hash, q.delivery_sequence,
           c.rule_code, 'warning'
    FROM issue_current_warnings c JOIN business.record_quality q USING (batch_id, record_id)
    UNION ALL
    SELECT batch_id, record_id, business_key_hash, delivery_sequence, reason_code,
           CASE WHEN decision='pending_reference' THEN 'pending' ELSE 'error' END
    FROM business.record_quality WHERE decision IN ('pending_reference', 'conflict', 'rejected')
)
SELECT DISTINCT sha256(to_json(list_value('quality_issue', batch_id, record_id, rule_code))) AS issue_id,
       * FROM observations;

-- Restrict accepted-state evaluation to keys that have an observed or retained issue.
CREATE TEMP TABLE issue_latest_accepted AS
SELECT q.batch_id, q.record_id, q.business_key_hash, q.delivery_sequence, q.operation
FROM business.record_quality q
WHERE q.decision='accepted' AND q.business_key_hash IN (
    SELECT business_key_hash FROM issue_observed
    UNION
    SELECT q.business_key_hash FROM business.quality_issues i
    JOIN business.record_quality q USING (batch_id, record_id)
)
QUALIFY row_number() OVER (PARTITION BY business_key_hash ORDER BY delivery_sequence DESC, record_id)=1;

CREATE TEMP TABLE issue_state AS
SELECT o.*,
       CASE WHEN o.severity='warning' THEN EXISTS (
           SELECT 1 FROM issue_current_warnings c
           WHERE c.batch_id=o.batch_id AND c.record_id=o.record_id AND c.rule_code=o.rule_code
       ) ELSE a.delivery_sequence IS NULL OR a.delivery_sequence<=o.delivery_sequence END AS is_active
FROM issue_observed o LEFT JOIN issue_latest_accepted a USING (business_key_hash);

-- Link only to the domain satellite, not enrollment or assignment satellites.
CREATE TEMP TABLE issue_domain_versions AS
SELECT l.batch_id, l.record_id, min(l.version_id) AS version_id
FROM vault.version_lineage l JOIN raw.source_records r USING (record_id)
WHERE (l.batch_id, l.record_id) IN (
    SELECT batch_id, record_id FROM issue_observed
    UNION SELECT batch_id, record_id FROM issue_latest_accepted
    UNION SELECT batch_id, record_id FROM business.quality_issues
) AND l.source_relation=CASE r.source_name
    WHEN 'participants' THEN 'vault.sat_participant_profile'
    WHEN 'device_metadata' THEN 'vault.sat_device_metadata'
    WHEN 'steps' THEN 'vault.sat_steps_observation'
    WHEN 'heart_rate' THEN 'vault.sat_heart_rate_observation'
    WHEN 'sleep' THEN 'vault.sat_sleep_session'
    WHEN 'wellness_survey' THEN 'vault.sat_wellness_response' END
GROUP BY l.batch_id, l.record_id;

INSERT INTO business.quality_issues
SELECT s.issue_id, s.batch_id, s.record_id, v.version_id, s.rule_code, s.severity, now(), 'open'
FROM issue_state s LEFT JOIN issue_domain_versions v USING (batch_id, record_id)
WHERE NOT EXISTS (SELECT 1 FROM business.quality_issues i WHERE i.issue_id=s.issue_id);

CREATE TEMP TABLE issue_transitions AS
SELECT i.issue_id, coalesce(s.is_active, false) AS is_active,
       CASE WHEN coalesce(s.is_active, false) THEN 'reopened'
            WHEN a.delivery_sequence>q.delivery_sequence AND a.operation='delete' THEN 'source_deleted'
            WHEN a.delivery_sequence>q.delivery_sequence THEN 'superseded_by_accepted_delta'
            WHEN i.severity='pending' AND q.decision='accepted' THEN 'reference_resolved'
            ELSE 'rule_cleared' END AS resolution_code,
       CASE WHEN NOT coalesce(s.is_active, false) THEN v.version_id END AS superseding_version_id
FROM business.quality_issues i
LEFT JOIN issue_state s USING (issue_id)
JOIN business.record_quality q ON q.batch_id=i.batch_id AND q.record_id=i.record_id
LEFT JOIN issue_latest_accepted a ON a.business_key_hash=q.business_key_hash
LEFT JOIN issue_domain_versions v ON v.batch_id=a.batch_id AND v.record_id=a.record_id
WHERE i.current_status<>CASE WHEN coalesce(s.is_active, false) THEN 'open' ELSE 'resolved' END
AND NOT (i.current_status='resolved' AND coalesce(s.is_active,false) AND EXISTS (
    SELECT 1 FROM business.quality_resolutions r JOIN publication_build.publications p USING(publication_id)
    WHERE r.issue_id=i.issue_id AND r.resolution_code='excluded_at_consolidation' AND p.status='published'
));
