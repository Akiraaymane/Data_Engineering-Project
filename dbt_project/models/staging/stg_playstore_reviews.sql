
with source as (
    select * from read_json_auto('c:/Users/mosta/Desktop/Data engineering/data/raw/reviews_raw.json')
),

apps as (
    select app_id from {{ ref('stg_playstore_apps') }} limit 1
),

renamed as (
    select
        reviewId as review_id,
        userName as user_name,
        userImage as user_image,
        content as review_text,
        score as rating,
        thumbsUpCount as thumbs_up_count,
        reviewCreatedVersion as review_created_version,
        -- 'at' is usually a timestamp string, need to handle parsing if json reader didn't auto-detect
        try_cast("at" as timestamp) as review_timestamp,
        replyContent as reply_content,
        repliedAt as replied_at,
        appVersion as app_version,
        
        -- Link to App (Using cross join assumption for single-app context)
        a.app_id
        
    from source
    cross join apps a
)

select * from renamed
