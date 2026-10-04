select business_key_hash from {{ ref('quality_current') }}
group by business_key_hash having count(*) <> 1
