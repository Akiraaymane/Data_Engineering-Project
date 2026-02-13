
import duckdb

sql_file = 'models/staging/stg_playstore_apps.sql'

try:
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    # Remove jinja refs if any (I replaced them, but just in case)
    # The file has no jinja now except maybe comments
    print(f"Executing SQL from {sql_file}...")
    
    con = duckdb.connect()
    con.sql(sql).show()
    print("Success!")
except Exception as e:
    print("Error:")
    print(e)
