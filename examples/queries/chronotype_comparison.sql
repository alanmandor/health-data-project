-- Descriptive comparisons use the latest published revision and its frozen cohort.
-- Missing measurements remain NULL; counts and coverage accompany every mean.
SELECT w.week_start, s.chronotype,
    count(DISTINCT d.participant_hk) AS participants,
    sum(d.expected_minutes) AS expected_minutes,
    sum(d.accepted_steps_minutes) AS accepted_steps_minutes,
    sum(d.observed_steps_total) AS observed_steps_total,
    sum(d.accepted_heart_rate_minutes) AS accepted_heart_rate_minutes,
    sum(d.heart_rate_sum) / nullif(sum(d.accepted_heart_rate_minutes), 0) AS observed_heart_rate_mean,
    count(d.reported_stage_sleep_min) AS reported_sleep_value_days,
    avg(d.reported_stage_sleep_min) AS reported_sleep_minutes_mean,
    count(d.efficiency_pct_reported) AS reported_efficiency_value_days,
    avg(d.efficiency_pct_reported) AS reported_efficiency_mean,
    count(d.fatigue_score) AS fatigue_response_days,
    avg(d.fatigue_score) AS fatigue_mean,
    sum(d.warning_count) AS accepted_warning_flags
FROM biostatistics.latest_participant_week w
JOIN biostatistics.participant_snapshot s USING (publication_id, participant_hk)
JOIN biostatistics.participant_day d USING (publication_id, participant_hk)
GROUP BY w.week_start, s.chronotype
ORDER BY w.week_start, s.chronotype;
