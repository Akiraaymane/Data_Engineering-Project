
import duckdb
import os

db_path = 'c:/Users/mosta/Desktop/Data engineering/data/duckdb/playstore.duckdb'
if os.path.exists(db_path):
    print(f"Connecting to {db_path}...")
    try:
        con = duckdb.connect(db_path)
        print("Tables:")
        con.sql("SHOW TABLES").show()
        
        # Check if stg_playstore_apps exists and has data
        try:
           count = con.sql("SELECT count(*) FROM stg_playstore_apps").fetchall()
           print(f"stg_playstore_apps count: {count}")
        except:
           print("stg_playstore_apps query failed.")
           
    except Exception as e:
        print(e)
else:
    print("Database file not found.")

print("\n--- LOG ---")
try:
    with open('dbt.log', 'r') as f:
        print(f.read())
except:
    print("Log file not found.")
