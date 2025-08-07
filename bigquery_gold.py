import pandas as pd
from google.cloud import bigquery
import os

# ------------------------------
# Step 1: Setup GCP Credentials
# ------------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./h-gcp-key.json"
project_id = "health-rcm"
dataset_id = "gold"
client = bigquery.Client(project=project_id)

# ------------------------------
# Step 2: Load SCD2 Patients CSV
# ------------------------------
scd_df = pd.read_csv("scd_type2_patients.csv")

# Convert date columns
date_cols = ["dob", "effective_date", "expiry_date"]
for col in date_cols:
    if col in scd_df.columns:
        scd_df[col] = pd.to_datetime(scd_df[col], errors="coerce")

# Ensure phone number is string
scd_df["phone_number"] = scd_df["phone_number"].astype(str)

# ------------------------------
# Step 3: Define Schema
# ------------------------------
scd_schema = [
    bigquery.SchemaField("patient_sk", "INTEGER"),
    bigquery.SchemaField("patient_id", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("middle_name", "STRING"),
    bigquery.SchemaField("phone_number", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("dob", "DATE"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("insurance", "FLOAT"),
    bigquery.SchemaField("effective_date", "DATE"),
    bigquery.SchemaField("expiry_date", "DATE"),
    bigquery.SchemaField("is_current", "BOOLEAN"),
    bigquery.SchemaField("version", "INTEGER")
]

# ------------------------------
# Step 4: Upload to BigQuery
# ------------------------------
table_id = f"{project_id}.{dataset_id}.dim_patients"

job_config = bigquery.LoadJobConfig(
    schema=scd_schema,
    write_disposition="WRITE_TRUNCATE"  # Replace old table data
)

job = client.load_table_from_dataframe(scd_df, table_id, job_config=job_config)
job.result()
print("✅ dim_patients (SCD2) uploaded to GOLD dataset.")
