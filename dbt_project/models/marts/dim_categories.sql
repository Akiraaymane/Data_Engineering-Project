
with unique_categories as (
    select distinct
        category_name
    from {{ ref('stg_playstore_apps') }}
    where category_name is not null
),
numbered as (
    select
        row_number() over (order by category_name) as category_key,
        category_name
    from unique_categories
)
select * from numbered
