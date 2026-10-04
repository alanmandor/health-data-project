{{ config(materialized='table') }}
with pending as (
    select distinct study_hk, participant_hk, local_date
    from business.mart_build_scope
), candidates as (
    select q.record_source, q.participant_id, q.source_name, q.device_id,
        q.height_cm, q.gender, q.chronotype, q.firmware_version, q.firmware_provenance,
        q.calibration_date, q.wear_site, q.event_at_utc,
        case when q.source_name in ('steps','heart_rate') then q.local_date else q.observation_date end as analysis_date,
        q.steps_normalized, q.heart_rate_bpm, q.sleep_onset_utc, q.sleep_end_utc,
        q.sleep_duration_min_reported, q.efficiency_pct_reported, q.restlessness,
        q.fatigue_score, q.stress_score, q.readiness_score, q.sleep_quality_score,
        q.quality_status, q.rule_version, v.participant_hk, v.study_hk,
        v.parent_hk, v.version_id, v.loaded_at,
        row_number() over (partition by q.business_key_hash order by v.delivery_sequence desc, v.version_id desc) as version_rank
    from business.quality_current q
    join business.vault_accepted_changes v
        on v.business_key_hash=q.business_key_hash and (
            (q.source_name='participants' and exists (
                select 1 from vault.sat_participant_profile s where s.version_id=v.version_id
                and s.height_cm is not distinct from q.height_cm and s.gender is not distinct from q.gender
                and s.chronotype is not distinct from q.chronotype))
            or (q.source_name='device_metadata' and exists (
                select 1 from vault.sat_device_metadata s where s.version_id=v.version_id
                and s.calibration_date is not distinct from q.calibration_date
                and s.firmware_version_reported is not distinct from q.firmware_version_reported
                and s.device_label is not distinct from q.device_label))
            or (q.source_name not in ('participants','device_metadata') and v.normalized_payload_hash=q.normalized_payload_hash))
        and v.delivery_sequence<=q.delivery_sequence
        and v.source_relation=case q.source_name
            when 'participants' then 'vault.sat_participant_profile'
            when 'device_metadata' then 'vault.sat_device_metadata'
            when 'steps' then 'vault.sat_steps_observation'
            when 'heart_rate' then 'vault.sat_heart_rate_observation'
            when 'sleep' then 'vault.sat_sleep_session'
            when 'wellness_survey' then 'vault.sat_wellness_response' end
    where q.source_name in ('participants','device_metadata') or exists (
        select 1 from pending p where p.study_hk=v.study_hk and p.participant_hk=v.participant_hk
        and p.local_date=case when q.source_name in ('steps','heart_rate') then q.local_date else q.observation_date end)
)
select * exclude(version_rank) from candidates where version_rank=1
