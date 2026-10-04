{{ config(materialized='table') }}
with keyed as (
    select s.*, d.stage_count, d.invalid_stage, d.duration_discrepancy,
        d.timeline_discrepancy, d.boundary_discrepancy, d.sleep_duration_min_reported,
        case when participant_id is null then null
             when source_name = 'device_metadata' and device_id is null then null
             when source_name in ('steps', 'heart_rate') and event_at_utc is null then null
             when source_name in ('sleep', 'wellness_survey') and observation_date is null then null
             else sha256(to_json(struct_pack(namespace := record_source, domain := source_name,
                  participant := participant_id,
                  device := case when source_name = 'device_metadata' then device_id end,
                  instant := case when source_name in ('steps', 'heart_rate') then epoch_us(event_at_utc) end,
                  day := case when source_name in ('sleep', 'wellness_survey') then observation_date end))) end as business_key_hash,
        case
            when operation not in ('upsert', 'delete') then 'invalid_operation'
            when participant_id is null then 'missing_participant_id'
            when source_name = 'device_metadata' and device_id is null then 'missing_device_id'
            when source_name in ('steps', 'heart_rate') and event_at_utc is null then 'invalid_timestamp'
            when source_name in ('steps', 'heart_rate') and epoch_us(event_at_utc) % 60000000 <> 0 then 'off_grid_timestamp'
            when source_name in ('sleep', 'wellness_survey') and observation_date is null then 'invalid_date'
            when operation = 'delete' then null
            when source_name = 'participants' and (device_id is null or age_at_enrollment is null or age_at_enrollment not between 0 and 32767
                or height_cm is null or height_cm <= 0 or gender is null or gender not in ('male', 'female')
                or chronotype is null or chronotype not in ('A', 'B') or max_heart_rate is null or max_heart_rate not between 1 and 32767) then 'invalid_participant_profile'
            when source_name = 'device_metadata' and (calibration_date is null or wear_site is null or firmware_version is null) then 'invalid_device_metadata'
            when source_name = 'steps' and (steps_reported is null or steps_reported < 0 or steps_normalized is null) then 'invalid_steps_or_unit'
            when source_name = 'heart_rate' and (heart_rate_bpm is null or heart_rate_bpm not between 40 and 200) then 'invalid_heart_rate'
            when source_name = 'wellness_survey' and (fatigue_score is null or fatigue_score not between 1 and 5
                or stress_score is null or stress_score not between 1 and 5
                or readiness_score is null or readiness_score not between 0 and 10
                or sleep_quality_score is null or sleep_quality_score not between 1 and 5) then 'invalid_wellness_scores'
            when source_name = 'sleep' and (sleep_onset_utc is null or sleep_end_utc is null or sleep_end_utc <= sleep_onset_utc
                or efficiency_pct_reported is null or efficiency_pct_reported not between 0 and 100
                or restlessness is null or restlessness not between 0 and 1
                or coalesce(stage_count, 0) = 0 or invalid_stage) then 'invalid_sleep_structure'
        end as invalid_reason_code,
        source_name = 'device_metadata' and firmware_version_reported is not null
            and firmware_version_from_label is not null
            and firmware_version_reported <> firmware_version_from_label as firmware_conflict,
        case when source_name = 'steps' and steps_normalized > 30 then ['high_steps_review']
             when source_name = 'sleep' then list_filter([
                 case when duration_discrepancy then 'sleep_duration_discrepancy' end,
                 case when timeline_discrepancy then 'sleep_timeline_discrepancy' end,
                 case when boundary_discrepancy then 'sleep_boundary_discrepancy' end
             ], x -> x is not null) else []::varchar[] end as warning_codes,
        case source_name
            when 'participants' then to_json(struct_pack(device := device_id, age := age_at_enrollment,
                height := height_cm, gender := gender, chronotype := chronotype, max_hr := max_heart_rate))
            when 'device_metadata' then to_json(struct_pack(wear_site := wear_site, calibration := calibration_date,
                firmware := firmware_version, label := device_label))
            when 'steps' then to_json(struct_pack(steps := steps_normalized))
            when 'heart_rate' then to_json(struct_pack(bpm := heart_rate_bpm))
            when 'wellness_survey' then to_json(struct_pack(fatigue := fatigue_score, stress := stress_score,
                readiness := readiness_score, sleep_quality := sleep_quality_score))
            when 'sleep' then to_json(struct_pack(onset := epoch_us(sleep_onset_utc), ending := epoch_us(sleep_end_utc),
                efficiency := efficiency_pct_reported, restlessness := restlessness, stages := d.normalized_stages))
        end::varchar as normalized_value
    from {{ ref('stg_records') }} s
    left join {{ ref('sleep_diagnostics') }} d using (batch_id, record_id)
), hashed as (
    select *, sha256(to_json(struct_pack(operation := operation,
        value := case when operation = 'delete' then null else normalized_value end,
        invalid_reason := invalid_reason_code))) as normalized_payload_hash
    from keyed
)
select *, count(distinct normalized_payload_hash) over (
    partition by batch_id, business_key_hash) > 1 and business_key_hash is not null as duplicate_conflict,
    row_number() over (partition by batch_id, business_key_hash, normalized_payload_hash order by record_id) as duplicate_rank,
    '{{ var("quality_rule_version") }}' as rule_version,
    '{{ var("code_version") }}' as code_version
from hashed
