import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="f1-analytics-db-instance-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com",
    port=5432,
    user="f1admin",
    password="formula1.1832",
    dbname="postgres"
)

for table in ["results", "lap_times"]:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    df.to_csv(f"data/{table}.csv", index=False)

conn.close()
