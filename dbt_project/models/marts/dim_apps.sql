
with apps as (
    select * from {{ ref('stg_playstore_apps') }}
),
developers as (
    select developer_key, developer_id from {{ ref('dim_developers') }}
),
categories as (
    select category_key, category_name from {{ ref('dim_categories') }}
),
numbered as (
    select
        row_number() over (order by a.app_id) as app_key,
        a.app_id,
        a.app_name,
        d.developer_key,
        c.category_key,
        a.price,
        a.is_paid,
        a.installs,
        a.catalog_rating,
        a.ratings_count
    from apps a
    left join developers d on a.developer_id = d.developer_id
    left join categories c on a.category_name = c.category_name
)
select * from numbered
