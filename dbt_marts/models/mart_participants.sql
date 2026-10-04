select sha256(to_json(list_value('participant_dimension',participant_hk,version_id))) as participant_sk,
    participant_hk, participant_id, record_source as business_namespace,
    height_cm, gender, chronotype, version_id as profile_version_id
from {{ ref('mart_records') }} where source_name='participants'
