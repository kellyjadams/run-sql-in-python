import pandas as pd
from google.cloud import bigquery

# BigQuery client
client = bigquery.Client()

# Perform a query
query = """
    SELECT id, timestamp 
    FROM `project-id-123.database-name.customers`
    LIMIT 100
"""

try:
    query_job = client.query(query)
    df = query_job.to_dataframe()

    # Print DataFrame
    print("Query results:")
    print(df)

except Exception as e:
    # Handle any exceptions
    print("An error occurred during query execution:", e)
