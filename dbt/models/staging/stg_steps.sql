select * from {{ ref('stg_records') }} where source_name = 'steps'
