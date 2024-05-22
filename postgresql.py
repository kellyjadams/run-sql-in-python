import psycopg2
from google.cloud import secretmanager
import pandas as pd
from sqlalchemy import create_engine

project_id = 'project-id-123'

# Secret Manager client
secretmanager_client = secretmanager.SecretManagerServiceClient()

# Secret names
secret_name_key = "SERVICE_ACCOUNT_KEY"

# Retrieve secrets from Secret Manager
request_key = {"name": f"projects/{project_id}/secrets/{secret_name_key}/versions/latest"}
response_key = secretmanager_client.access_secret_version(request_key)
secret_string_key = response_key.payload.data.decode("UTF-8")  

# Connect to DB
conn_params = {
    "host": "host-name",
    "database": "database-name",
    "user": "user-name",
    "password": secret_string_key}

# Construct database connection string for SQLAlchemy
db_uri = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['database']}"

# Create SQLAlchemy engine
engine = create_engine(db_uri)

try:
    # Execute SQL query using SQLAlchemy engine with parameterization
    query = """
        SELECT id, timestamp
        FROM customers
        WHERE timestamp::date = %(date)s
        """
    df = pd.read_sql_query(query, engine, params={'date': '2024-05-14'})
    
    # Print query results
    print(df)

except Exception as e:
    # Handle any exceptions
    print("An error occurred:", e)

