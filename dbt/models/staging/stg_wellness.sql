select *, observation_date as survey_date from {{ ref('stg_records') }} where source_name = 'wellness_survey'
