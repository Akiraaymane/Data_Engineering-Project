
{{
    config(
        materialized='incremental',
        unique_key='review_id'
    )
}}

with reviews as (
    select * from {{ ref('stg_playstore_reviews') }}
),
apps as (
    select app_key, app_id, developer_key from {{ ref('dim_apps') }}
),
dates as (
    select date_key, date from {{ ref('dim_date') }}
)
select
    row_number() over (order by r.review_id)   as review_id,
    a.app_key,
    a.developer_key,
    d.date_key,

    cast(r.rating as integer)           as rating,
    cast(r.thumbs_up_count as integer)  as thumbs_up_count,
    r.review_text,
    r.review_version

from reviews r
inner join apps  a on r.app_id = a.app_id
inner join dates d on cast(r.review_timestamp as date) = d.date

{% if is_incremental() %}
  where cast(r.review_timestamp as date) > (
    select max(dt.date)
    from {{ this }} f
    join {{ ref('dim_date') }} dt on f.date_key = dt.date_key
  )
{% endif %}
