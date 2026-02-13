
import duckdb
import os

db_path = 'c:/Users/mosta/Desktop/Data engineering/data/duckdb/playstore.duckdb'

tables = [
    'dim_developers',
    'dim_categories',
    'dim_apps',
    'dim_date',
    'fact_reviews',
    'apps_snapshot',
    'dim_apps_scd'
]

print("## Verification Counts")
try:
    con = duckdb.connect(db_path)
    for t in tables:
        try:
            count = con.sql(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"- **{t}**: {count} rows")
        except:
            print(f"- **{t}**: (Error querying)")
except Exception as e:
    print(e)
