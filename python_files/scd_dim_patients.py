import pandas as pd
import hashlib
from datetime import datetime
from google.cloud import bigquery


PROJECT_ID = "rcm-project-467805"  
DATASET_ID = "healthcare_rcm"
TABLE_ID = "dim_patients"


new_data = pd.read_csv("combined_cleaned_patients.csv")  


new_data['effective_date'] = pd.to_datetime(datetime.today().date())
new_data['expiry_date'] = pd.NaT
new_data['is_current'] = True
new_data['version'] = 1


def hash_row(row):
    key = f"{row['first_name']}{row['last_name']}{row['middle_name']}{row['phone_number']}{row['dob']}{row['gender']}"
    return hashlib.md5(key.encode()).hexdigest()

new_data['hash_key'] = new_data.apply(hash_row, axis=1)


client = bigquery.Client()
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
query = f"SELECT * FROM `{table_ref}` WHERE is_current = TRUE"
existing_data = client.query(query).to_dataframe()


merged = pd.merge(new_data, existing_data, on="patient_id", how="left", suffixes=("_new", "_old"))


changed_records = merged[
    (merged['hash_key_new'] != merged['hash_key_old']) |
    (merged['hash_key_old'].isnull())
]


expired_records = existing_data[
    existing_data['patient_id'].isin(changed_records['patient_id'])
].copy()

expired_records['expiry_date'] = datetime.today().date()
expired_records['is_current'] = False


final_new_records = new_data[
    new_data['patient_id'].isin(changed_records['patient_id'])
].copy()

final_new_records['version'] = 1
final_new_records['is_current'] = True
final_new_records['expiry_date'] = pd.NaT
final_new_records['effective_date'] = datetime.today().date()


final_df = pd.concat([expired_records, final_new_records], ignore_index=True)


final_df = final_df.drop(columns=["hash_key"])


job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
job.result()

print("✅ SCD Type 2 updates loaded to BigQuery.")
