
with apps as (
    select * from {{ ref('stg_playstore_apps') }}
),
developers as (
    select * from {{ ref('dim_developers') }}
),
categories as (
    select * from {{ ref('dim_categories') }}
)
select
    md5(cast(a.app_id as varchar)) as app_key,
    a.app_id,
    a.app_name,
    a.rating,
    a.reviews_count,
    a.installs_range,
    a.min_installs,
    a.price,
    a.is_free,
    a.content_rating,
    a.last_updated,
    a.current_version,
    
    d.developer_key,
    c.category_key
    
from apps a
left join developers d on a.developer_id = d.developer_id
left join categories c on a.category_name = c.category_name
