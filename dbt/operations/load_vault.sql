-- Executed inside the loader's transaction. Model SQL prepares source versions;
-- this operation appends persistent foundation objects without rebuilding them.
CREATE TEMP TABLE dv_versions AS SELECT * FROM staging.dv_satellite_candidates;

INSERT INTO vault.hub_participant
SELECT participant_hk, record_source, participant_id, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND NOT EXISTS (
    SELECT 1 FROM vault.hub_participant h WHERE h.participant_hk=i.participant_hk)
QUALIFY row_number() OVER (PARTITION BY participant_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.hub_device
SELECT device_hk, record_source, device_id, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND device_hk IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM vault.hub_device h WHERE h.device_hk=i.device_hk)
QUALIFY row_number() OVER (PARTITION BY device_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.hub_sleep_session
SELECT sleep_session_hk, study_hk, participant_hk, observation_date, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND source_name='sleep' AND NOT EXISTS (
    SELECT 1 FROM vault.hub_sleep_session h WHERE h.sleep_session_hk=i.sleep_session_hk)
QUALIFY row_number() OVER (PARTITION BY sleep_session_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.hub_wellness_response
SELECT wellness_response_hk, study_hk, participant_hk, observation_date, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND source_name='wellness_survey' AND NOT EXISTS (
    SELECT 1 FROM vault.hub_wellness_response h WHERE h.wellness_response_hk=i.wellness_response_hk)
QUALIFY row_number() OVER (PARTITION BY wellness_response_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.link_participant_device
SELECT participant_device_hk, participant_hk, device_hk, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND participant_device_hk IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM vault.link_participant_device l WHERE l.participant_device_hk=i.participant_device_hk)
QUALIFY row_number() OVER (PARTITION BY participant_device_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.link_participant_study
SELECT participant_study_hk, participant_hk, study_hk, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND source_name='participants' AND NOT EXISTS (
    SELECT 1 FROM vault.link_participant_study l WHERE l.participant_study_hk=i.participant_study_hk)
QUALIFY row_number() OVER (PARTITION BY participant_study_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.link_participant_sleep_session
SELECT participant_sleep_hk, participant_hk, sleep_session_hk, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND source_name='sleep' AND NOT EXISTS (
    SELECT 1 FROM vault.link_participant_sleep_session l WHERE l.participant_sleep_hk=i.participant_sleep_hk)
QUALIFY row_number() OVER (PARTITION BY participant_sleep_hk ORDER BY delivery_sequence, batch_id, record_id)=1;

INSERT INTO vault.link_participant_wellness_response
SELECT participant_wellness_hk, participant_hk, wellness_response_hk, batch_id, record_source, now(), 'dv-json-array-v1'
FROM staging.dv_record_input i
WHERE structurally_loadable AND source_name='wellness_survey' AND NOT EXISTS (
    SELECT 1 FROM vault.link_participant_wellness_response l WHERE l.participant_wellness_hk=i.participant_wellness_hk)
QUALIFY row_number() OVER (PARTITION BY participant_wellness_hk ORDER BY delivery_sequence, batch_id, record_id)=1;
