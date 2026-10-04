-- This model reads frozen publication rows, never mutable current facts.
select d.publication_id, d.participant_hk, d.study_hk, p.week_start,
    count(*)::integer as expected_days, sum(d.expected_minutes)::integer as expected_minutes,
    sum(d.accepted_steps_minutes)::integer as accepted_steps_minutes,
    sum(d.accepted_heart_rate_minutes)::integer as accepted_heart_rate_minutes,
    sum(d.observed_steps_total) as observed_steps_total,
    sum(d.heart_rate_sum)::integer as heart_rate_sum,
    sum(d.heart_rate_sum)/nullif(sum(d.accepted_heart_rate_minutes),0) as heart_rate_mean,
    count(*) filter(where d.has_sleep)::integer as accepted_sleep_sessions,
    sum(d.reported_stage_sleep_min) as sleep_minutes_sum,
    avg(d.reported_stage_sleep_min) as sleep_minutes_mean,
    sum(d.deep_sleep_pct_from_reported_stages) as deep_sleep_pct_sum,
    count(d.deep_sleep_pct_from_reported_stages)::integer as deep_sleep_pct_count,
    avg(d.deep_sleep_pct_from_reported_stages) as deep_sleep_pct_mean,
    sum(d.efficiency_pct_reported) as efficiency_pct_sum,
    count(d.efficiency_pct_reported)::integer as efficiency_pct_count,
    avg(d.efficiency_pct_reported) as efficiency_pct_mean,
    count(*) filter(where d.has_wellness)::integer as accepted_wellness_responses,
    sum(d.fatigue_score) as fatigue_sum, count(d.fatigue_score)::integer as fatigue_count, avg(d.fatigue_score) as fatigue_mean,
    sum(d.stress_score) as stress_sum, count(d.stress_score)::integer as stress_count, avg(d.stress_score) as stress_mean,
    sum(d.readiness_score) as readiness_sum, count(d.readiness_score)::integer as readiness_count, avg(d.readiness_score) as readiness_mean,
    sum(d.sleep_quality_score) as sleep_quality_sum, count(d.sleep_quality_score)::integer as sleep_quality_count, avg(d.sleep_quality_score) as sleep_quality_mean,
    sum(d.warning_count)::integer as warning_count, count(*)<7 as partial_week,
    case when sum(d.warning_count)>0 then 'accepted_with_warning' else 'accepted' end as quality_status
from publication_build.participant_day d join publication_build.publications p using(publication_id)
where d.publication_id='{{ var("publication_id") }}'
group by d.publication_id,d.participant_hk,d.study_hk,p.week_start
