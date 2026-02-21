
{% snapshot apps_snapshot %}

{{
    config(
      target_schema='main',
      unique_key='app_id',
      strategy='check',
      check_cols=['catalog_rating', 'ratings_count', 'price', 'installs'],
    )
}}

select * from {{ ref('stg_playstore_apps') }}

{% endsnapshot %}
