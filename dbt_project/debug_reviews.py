
import duckdb
import os

db_path = 'c:/Users/mosta/Desktop/Data engineering/data/duckdb/playstore.duckdb'

sql = """
with source as (
    select * from read_json_auto('c:/Users/mosta/Desktop/Data engineering/data/raw/reviews_raw.json')
),

apps as (
    select app_id from stg_playstore_apps limit 1
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
        try_cast(at as timestamp) as review_timestamp,
        replyContent as reply_content,
        repliedAt as replied_at,
        appVersion as app_version,
        
        a.app_id
        
    from source
    cross join apps a
)

select * from renamed limit 5
"""

try:
    print("Connecting...")
    con = duckdb.connect(db_path)
    print("Running SQL...")
    con.sql(sql).show()
    print("Success!")
except Exception as e:
    print("Error:")
    print(e)
