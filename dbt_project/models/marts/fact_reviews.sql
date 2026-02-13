
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
    select * from {{ ref('dim_apps') }}
),
dates as (
    select * from {{ ref('dim_date') }}
)
select
    md5(cast(r.review_id as varchar)) as review_key,
    r.review_id,
    r.rating,
    r.thumbs_up_count,
    r.review_text,
    
    a.app_key,
    coalesce(d.date_key, -1) as review_date_key
    
from reviews r
left join apps a on r.app_id = a.app_id
left join dates d on cast(r.review_timestamp as date) = d.date_day

{% if is_incremental() %}
  where cast(r.review_timestamp as date) > (select max(d.date_day) from {{ this }} f join {{ ref('dim_date') }} d on f.review_date_key = d.date_key)
{% endif %}
