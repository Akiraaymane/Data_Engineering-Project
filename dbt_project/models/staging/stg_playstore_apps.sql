
with source as (
    select * from read_json_auto('c:/Users/mosta/Desktop/Data engineering/data/raw/apps_raw.json')
),

renamed as (
    select
        appId as app_id,
        title as app_name,
        genreId as category_id,
        genre as category_name,
        developerId as developer_id,
        developer as developer_name,
        developerEmail as developer_email,
        developerWebsite as developer_website,
        
        -- Metrics
        score as catalog_rating,
        reviews as ratings_count,
        installs,
        minInstalls as min_installs,
        realInstalls as real_installs,
        price,
        not free as is_paid,
        currency,
        
        -- Details
        contentRating as content_rating,
        description,
        summary,
        released as released_date,
        to_timestamp(updated) as last_updated,
        version as current_version,
        url as playstore_url
        
    from source
)

select * from renamed
