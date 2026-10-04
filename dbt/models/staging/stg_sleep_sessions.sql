select *, observation_date as wake_date from {{ ref('stg_records') }} where source_name = 'sleep'
