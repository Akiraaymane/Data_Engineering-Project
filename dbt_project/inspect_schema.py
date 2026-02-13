
import duckdb
try:
    con = duckdb.connect()
    # Read apps
    apps_df = con.sql("SELECT * FROM read_json_auto('../data/raw/apps_raw.json') LIMIT 1").df()
    print("APPS COLUMNS:")
    for col in apps_df.columns:
        print(f"- {col}")
    
    # Read reviews
    reviews_df = con.sql("SELECT * FROM read_json_auto('../data/raw/reviews_raw.json') LIMIT 1").df()
    print("REVIEWS COLUMNS:")
    for col in reviews_df.columns:
        print(f"- {col}")
except Exception as e:
    print(e)
