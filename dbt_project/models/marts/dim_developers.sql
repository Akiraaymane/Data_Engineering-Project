
with unique_developers as (
    select distinct
        developer_id,
        developer_name
    from {{ ref('stg_playstore_apps') }}
    where developer_id is not null
)
select
    md5(cast(developer_id as varchar)) as developer_key,
    developer_id,
    developer_name
from unique_developers
