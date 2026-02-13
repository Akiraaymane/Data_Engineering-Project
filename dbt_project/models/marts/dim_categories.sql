
with unique_categories as (
    select distinct
        category_name,
        category_id
    from {{ ref('stg_playstore_apps') }}
    where category_name is not null
)
select
    md5(cast(category_name as varchar)) as category_key,
    category_id,
    category_name
from unique_categories
