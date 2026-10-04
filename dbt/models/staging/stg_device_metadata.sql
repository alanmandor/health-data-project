select * from {{ ref('stg_records') }} where source_name = 'device_metadata'
