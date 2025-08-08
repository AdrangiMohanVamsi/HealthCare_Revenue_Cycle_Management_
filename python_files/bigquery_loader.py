import pandas as pd
from google.cloud import bigquery
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./h-gcp-key.json"
project_id = "health-rcm"
dataset_id = "silver"
client = bigquery.Client(project=project_id)


def load_to_bigquery(df, table_name, schema=None):
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(schema=schema) if schema else None
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ {table_name} uploaded to BigQuery.")


dim_tables = {
    "dim_patients": "combined_cleaned_patients.csv",
    "dim_providers": "combined_cleaned_providers.csv",
    "dim_departments": "combined_cleaned_departments.csv",
    "dim_transactions": "combined_cleaned_transactions.csv",
    "dim_encounters": "combined_cleaned_encounters.csv"
}

for table_name, file_path in dim_tables.items():
    df = pd.read_csv(file_path)
    load_to_bigquery(df, table_name)


claims_df = pd.read_csv("combined_cleaned_claims.csv")


date_columns = ["service_date", "claim_date", "insert_date", "modified_date"]
for col in date_columns:
    if col in claims_df.columns:
        claims_df[col] = pd.to_datetime(claims_df[col], errors="coerce")

claims_schema = [
    bigquery.SchemaField("claim_id", "STRING"),
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("patient_id", "STRING"),
    bigquery.SchemaField("encounter_id", "STRING"),
    bigquery.SchemaField("provider_id", "STRING"),
    bigquery.SchemaField("dept_id", "STRING"),
    bigquery.SchemaField("service_date", "TIMESTAMP"),
    bigquery.SchemaField("claim_date", "TIMESTAMP"),
    bigquery.SchemaField("payor_id", "STRING"),
    bigquery.SchemaField("claim_amount", "FLOAT"),
    bigquery.SchemaField("paid_amount", "FLOAT"),
    bigquery.SchemaField("claim_status", "STRING"),
    bigquery.SchemaField("payor_type", "STRING"),
    bigquery.SchemaField("deductible", "FLOAT"),
    bigquery.SchemaField("coinsurance", "FLOAT"),
    bigquery.SchemaField("copay", "FLOAT"),
    bigquery.SchemaField("insert_date", "TIMESTAMP"),
    bigquery.SchemaField("modified_date", "TIMESTAMP")
]

load_to_bigquery(claims_df, "fact_claims", claims_schema)


cpt_df = pd.read_csv("combined_cleaned_procedures.csv")


cpt_df = cpt_df.rename(columns={
    "description": "procedure_description",
    "category": "procedure_code_category",
    "status": "code_status"
})


cpt_df = cpt_df[["procedure_sk", "cpt_code", "procedure_description", "procedure_code_category", "code_status"]]

cpt_schema = [
    bigquery.SchemaField("procedure_sk", "INTEGER"),
    bigquery.SchemaField("cpt_code", "STRING"),
    bigquery.SchemaField("procedure_description", "STRING"),
    bigquery.SchemaField("procedure_code_category", "STRING"),
    bigquery.SchemaField("code_status", "STRING")
]


scd_df = pd.read_csv("scd_type2_patients.csv")


for col in ['effective_date', 'expiry_date']:
    if col in scd_df.columns:
        scd_df[col] = pd.to_datetime(scd_df[col], errors='coerce')
scd_df["phone_number"] = scd_df["phone_number"].astype(str)
scd_df["full_name"] = scd_df["first_name"].str.strip() + " " + scd_df["last_name"].str.strip()
scd_df["dob"] = pd.to_datetime(scd_df["dob"], errors='coerce').dt.date
scd_df["insurance"] = scd_df["insurance"].astype(str)

# Schema
scd_schema = [
    bigquery.SchemaField("patient_sk", "INTEGER"),
    bigquery.SchemaField("patient_id", "STRING"),
    bigquery.SchemaField("full_name", "STRING"),  # ✅ This is now derived
    bigquery.SchemaField("dob", "DATE"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("phone_number", "STRING"),
    bigquery.SchemaField("insurance", "STRING"),
    bigquery.SchemaField("effective_date", "DATE"),
    bigquery.SchemaField("expiry_date", "DATE"),
    bigquery.SchemaField("is_current", "BOOLEAN"),
    bigquery.SchemaField("version", "INTEGER")
]


# Upload
table_id = f"{project_id}.{dataset_id}.dim_patients"
job = client.load_table_from_dataframe(scd_df, table_id, job_config=bigquery.LoadJobConfig(schema=scd_schema, write_disposition="WRITE_TRUNCATE"))
job.result()
print("✅ dim_patients (SCD Type 2) uploaded successfully.")  