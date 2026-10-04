with eligible as (
    select *, count(distinct to_json(list_value(firmware_version, firmware_provenance, calibration_date::varchar)))
        over(partition by parent_hk) as states,
        row_number() over(partition by parent_hk order by loaded_at desc, version_id desc) as device_rank
    from {{ ref('mart_records') }} where source_name='device_metadata'
)
select sha256(to_json(list_value('device_dimension',parent_hk,version_id))) as device_sk,
    parent_hk as device_hk, device_id, firmware_version, firmware_provenance,
    calibration_date, version_id as device_version_id
from eligible where states=1 and device_rank=1
