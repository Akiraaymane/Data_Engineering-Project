
{% snapshot apps_snapshot %}

{{
    config(
      target_schema='main',
      unique_key='app_id',
      strategy='check',
      check_cols=['rating', 'reviews_count', 'price', 'current_version', 'last_updated'],
    )
}}

select * from {{ ref('stg_playstore_apps') }}

{% endsnapshot %}
