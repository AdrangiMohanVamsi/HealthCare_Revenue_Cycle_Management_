import pandas as pd
from google.cloud import bigquery
import os

# Set your Google credentials and project info
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./h-gcp-key.json"  # Path to your service account key
project_id = "health-rcm"
dataset_id = "silver"
table_id = "dim_patients_scd"

# Load the SCD patient CSV
df = pd.read_csv("dim_patients_scd.csv")

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)

# Load data to BigQuery
job = client.load_table_from_dataframe(df, table_ref)
job.result()  # Wait for the job to complete

print("✅ SCD Type 2 dim_patients_scd successfully loaded to BigQuery.")
