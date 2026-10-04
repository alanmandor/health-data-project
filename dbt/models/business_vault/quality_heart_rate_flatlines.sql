{{ config(materialized='table') }}
-- Only validated current minutes participate; gaps and value changes split runs.
with previous as (
    select business_key_hash, record_source, participant_id, event_at_utc, heart_rate_bpm,
        lag(event_at_utc) over w as preceding_at,
        lag(heart_rate_bpm) over w as preceding_bpm
    from {{ ref('quality_current_base') }} where source_name = 'heart_rate'
    window w as (partition by record_source, participant_id order by event_at_utc)
), boundaries as (
    select *, case when preceding_at = event_at_utc - interval 1 minute
        and preceding_bpm = heart_rate_bpm then 0 else 1 end as starts_run
    from previous
), runs as (
    select *, sum(starts_run) over (partition by record_source, participant_id order by event_at_utc) as run_id
    from boundaries
), sized as (
    select business_key_hash, count(*) over (partition by record_source, participant_id, run_id) as run_minutes
    from runs
)
select * from sized where run_minutes >= 60
