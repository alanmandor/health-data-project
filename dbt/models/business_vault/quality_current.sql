-- A later rejected/conflicting candidate cannot replace an accepted version.
-- Select tombstones before removing them to prevent resurrection.
select c.* exclude (warning_codes, quality_status),
    case when f.business_key_hash is not null then list_append(c.warning_codes, 'suspected_heart_rate_flatline')
         else c.warning_codes end as warning_codes,
    case when f.business_key_hash is not null or len(c.warning_codes) > 0 then 'accepted_with_warning'
         else 'accepted' end as quality_status
from {{ ref('quality_current_base') }} c
left join {{ ref('quality_heart_rate_flatlines') }} f using (business_key_hash)
