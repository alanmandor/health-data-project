with latest as (
    select * from {{ ref('record_quality') }} where decision = 'accepted'
    qualify row_number() over (partition by business_key_hash order by delivery_sequence desc) = 1
)
select * from latest where operation = 'upsert'
