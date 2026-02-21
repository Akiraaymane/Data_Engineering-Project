
with unique_developers as (
    select distinct
        developer_id,
        developer_name,
        developer_email,
        developer_website
    from {{ ref('stg_playstore_apps') }}
    where developer_id is not null
),
numbered as (
    select
        row_number() over (order by developer_id) as developer_key,
        developer_id,
        developer_name,
        developer_website,
        developer_email
    from unique_developers
)
select * from numbered
