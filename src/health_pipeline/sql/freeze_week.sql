-- Parameters are supplied through the temporary freeze_context table.
INSERT INTO publication_build.participant_day
SELECT c.publication_id,d.participant_hk,d.study_hk,d.local_date,
    d.* EXCLUDE(study_hk,participant_hk,local_date)
FROM analytics.participant_day d CROSS JOIN freeze_context c
WHERE d.study_hk=c.study_hk AND d.local_date>=c.first_day AND d.local_date<=c.last_day;

CREATE TEMP TABLE freeze_enrollment AS
SELECT v.participant_hk,v.version_id,s.age_at_enrollment,s.max_heart_rate
FROM business.vault_current_versions v JOIN vault.sat_enrollment s USING(version_id)
WHERE v.source_relation='vault.sat_enrollment';

CREATE TEMP TABLE freeze_assignment AS
SELECT v.participant_hk,v.version_id FROM business.vault_current_versions v
JOIN vault.sat_participant_device s USING(version_id)
JOIN vault.link_participant_device l ON l.participant_device_hk=s.parent_hk
JOIN vault.hub_device d USING(device_hk)
JOIN marts.gold_participant_devices g ON g.participant_hk=v.participant_hk
    AND g.device_id=d.device_id AND g.business_namespace=d.business_namespace
    AND g.wear_site IS NOT DISTINCT FROM s.wear_site
WHERE v.source_relation='vault.sat_participant_device'
QUALIFY row_number() OVER(PARTITION BY v.participant_hk ORDER BY v.delivery_sequence DESC,v.version_id DESC)=1;

INSERT INTO publication_build.participant_snapshot
SELECT c.publication_id,p.participant_hk,p.participant_id,p.business_namespace,c.study_hk,
    p.chronotype,p.gender,p.height_cm,e.age_at_enrollment,e.max_heart_rate,p.profile_version_id,e.version_id,
    g.device_id,a.version_id,
    CASE WHEN g.assignment_quality_status='reported_assignment' THEN 'accepted' ELSE 'accepted_with_warning' END
FROM marts.dim_participant p CROSS JOIN freeze_context c
JOIN marts.gold_participant_devices g USING(participant_hk)
LEFT JOIN freeze_enrollment e USING(participant_hk)
LEFT JOIN freeze_assignment a USING(participant_hk)
WHERE p.is_current AND EXISTS(SELECT 1 FROM publication_build.participant_day d
    WHERE d.publication_id=c.publication_id AND d.participant_hk=p.participant_hk);

-- Preserve every provisional stage at the cutoff before applying exclusions.
INSERT INTO publication_build.sleep_stage_candidates
SELECT c.publication_id,t.* FROM marts.fact_sleep_stage t
JOIN marts.fact_sleep_session s USING(session_version_id) CROSS JOIN freeze_context c
WHERE s.study_hk=c.study_hk AND s.wake_date>=c.first_day AND s.wake_date<=c.last_day;

-- Only a terminal awake interval wholly after sleep_end is eligible for exclusion.
-- Internal awakenings followed by more sleep are retained even if bounds disagree.
INSERT INTO publication_build.sleep_stage_exclusions
SELECT c.publication_id,t.session_version_id,t.stage_position,'unresolved_post_boundary_awake',
    i.issue_id,sha256(to_json(list_value('consolidation_resolution',c.publication_id,i.issue_id)))
FROM marts.fact_sleep_stage t JOIN marts.fact_sleep_session s USING(session_version_id)
JOIN vault.hub_participant h ON h.participant_hk=s.participant_hk
JOIN business.quality_current q ON q.source_name='sleep' AND q.record_source=h.business_namespace
    AND q.participant_id=h.participant_id AND q.observation_date=s.wake_date
JOIN business.quality_issues i ON i.batch_id=q.batch_id AND i.record_id=q.record_id
    AND i.rule_code='sleep_boundary_discrepancy'
CROSS JOIN freeze_context c
WHERE s.study_hk=c.study_hk AND s.wake_date>=c.first_day AND s.wake_date<=c.last_day
    AND t.is_post_boundary_awake AND t.start_at_utc>=s.sleep_end_utc
    AND NOT EXISTS(SELECT 1 FROM marts.fact_sleep_stage later
        WHERE later.session_version_id=t.session_version_id AND later.stage_position>t.stage_position
        AND later.stage_type<>'awake');

-- Input manifests identify exactly the immutable versions selected by current facts.
INSERT INTO publication_build.inputs
SELECT c.publication_id,'profile',s.version_id,'vault.sat_participant_profile',s.parent_hk,s.payload_hash
FROM vault.sat_participant_profile s JOIN publication_build.participant_snapshot p ON p.profile_version_id=s.version_id
JOIN freeze_context c ON p.publication_id=c.publication_id;

INSERT INTO publication_build.inputs
SELECT c.publication_id,'enrollment',s.version_id,'vault.sat_enrollment',s.parent_hk,s.payload_hash
FROM vault.sat_enrollment s JOIN publication_build.participant_snapshot p ON p.enrollment_version_id=s.version_id
JOIN freeze_context c ON p.publication_id=c.publication_id;

INSERT INTO publication_build.inputs
SELECT c.publication_id,'assignment',s.version_id,'vault.sat_participant_device',s.parent_hk,s.payload_hash
FROM vault.sat_participant_device s JOIN publication_build.participant_snapshot p ON p.assignment_version_id=s.version_id
JOIN freeze_context c ON p.publication_id=c.publication_id;

INSERT INTO publication_build.inputs
SELECT DISTINCT c.publication_id,'device',s.version_id,'vault.sat_device_metadata',s.parent_hk,s.payload_hash
FROM marts.dim_device d JOIN vault.sat_device_metadata s ON s.version_id=d.device_version_id
JOIN vault.hub_device h ON h.device_hk=d.device_hk
JOIN publication_build.participant_snapshot p ON p.device_id=h.device_id AND p.business_namespace=h.business_namespace
JOIN freeze_context c ON p.publication_id=c.publication_id WHERE d.is_current;
