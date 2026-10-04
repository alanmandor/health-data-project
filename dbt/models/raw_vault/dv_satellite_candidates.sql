{% set definitions = [
    ('participant_profile', 'participants', 'participant_hk', [
        ('height_cm', 'height_cm'), ('gender', 'gender'), ('chronotype', 'chronotype')]),
    ('enrollment', 'participants', 'participant_study_hk', [
        ('age_at_enrollment', 'age_at_enrollment'), ('max_heart_rate', 'max_heart_rate'),
        ('enrollment_status', "'reported'"), ('enrolled_on', 'null::date'), ('withdrawn_on', 'null::date'),
        ('coverage_start', 'null::date'), ('coverage_end_exclusive', 'null::date'), ('coverage_basis', "'not_supplied'")]),
    ('participant_device', 'participants', 'participant_device_hk', [
        ('wear_site', 'null::varchar'), ('assignment_status', "'reported'"),
        ('effective_from', 'null::timestamptz'), ('effective_to', 'null::timestamptz')]),
    ('device_metadata', 'device_metadata', 'device_hk', [
        ('calibration_date', 'calibration_date'), ('firmware_version_reported', 'firmware_version_reported'), ('device_label', 'device_label')]),
    ('participant_device', 'device_metadata', 'participant_device_hk', [
        ('wear_site', 'wear_site'), ('assignment_status', "'reported'"),
        ('effective_from', 'null::timestamptz'), ('effective_to', 'null::timestamptz')]),
    ('steps_observation', 'steps', 'participant_hk', [
        ('study_hk', 'study_hk'), ('event_at_utc', 'event_at_utc'), ('timestamp_original', 'timestamp_original'),
        ('steps_reported', 'steps_reported'), ('unit_reported', 'unit_reported')]),
    ('heart_rate_observation', 'heart_rate', 'participant_hk', [
        ('study_hk', 'study_hk'), ('event_at_utc', 'event_at_utc'), ('timestamp_original', 'timestamp_original'), ('heart_rate_bpm', 'heart_rate_bpm')]),
    ('sleep_session', 'sleep', 'sleep_session_hk', [
        ('sleep_onset_utc', 'sleep_onset_utc'), ('sleep_end_utc', 'sleep_end_utc'),
        ('sleep_onset_original', 'sleep_onset_original'), ('sleep_end_original', 'sleep_end_original'),
        ('efficiency_pct_reported', 'efficiency_pct_reported'), ('restlessness', 'restlessness')]),
    ('wellness_response', 'wellness_survey', 'wellness_response_hk', [
        ('fatigue_score', 'fatigue_score'), ('stress_score', 'stress_score'),
        ('readiness_score', 'readiness_score'), ('sleep_quality_score', 'sleep_quality_score')])
] %}
with payloads as (
{% for relation, source, parent, fields in definitions %}
    select 'vault.sat_{{ relation }}' as source_relation, {{ parent }} as parent_hk,
        batch_id, delivery_sequence, record_source, record_id, study_hk,
        business_key_hash, decision, reason_code, rule_version, code_version, normalized_payload_hash,
        operation,
        {% if source in ['steps', 'heart_rate'] %}event_at_utc{% else %}null::timestamptz{% endif %} as source_effective_at,
        {% if source in ['steps', 'heart_rate'] %}strftime(event_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ'){% else %}null::varchar{% endif %} as discriminator,
        to_json(struct_pack(
        {% for name, expression in fields %}
            {{ name }} := {% if name in ['study_hk', 'event_at_utc'] %}{{ expression }}{% else %}case when operation = 'upsert' then {{ expression }} end{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
        )) as payload,
        {% if source == 'sleep' %}case when operation = 'upsert' then normalized_value else null end{% else %}null::varchar{% endif %} as complete_session_hash_input
    from {{ ref('dv_record_input') }}
    where source_name = '{{ source }}' and structurally_loadable and {{ parent }} is not null
    {% if not loop.last %}union all{% endif %}
{% endfor %}
), hashed as (
    select *, {{ dv_hash('source_payload', ['operation', 'payload', 'complete_session_hash_input']) }} as payload_hash
    from payloads
)
select *, {{ dv_hash('satellite_version', ['source_relation', 'parent_hk', 'study_hk', 'business_key_hash', 'discriminator', 'batch_id', 'operation', 'payload_hash']) }} as version_id,
    'dv-json-array-v1' as hash_spec_version
from hashed
