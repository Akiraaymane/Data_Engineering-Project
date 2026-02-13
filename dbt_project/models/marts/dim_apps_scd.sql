
with scd_data as (
    select * from {{ ref('apps_snapshot') }}
)
select
    md5(cast(app_id as varchar) || cast(dbt_updated_at as varchar)) as app_ver_key,
    *
from scd_data
