import pandas as pd
from google.cloud import bigquery
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./h-gcp-key.json"  
project_id = "health-rcm"
dataset_id = "silver"
table_id = "dim_patients_scd"


df = pd.read_csv("dim_patients_scd.csv")


client = bigquery.Client(project=project_id)


table_ref = client.dataset(dataset_id).table(table_id)


job = client.load_table_from_dataframe(df, table_ref)
job.result()  

print("✅ SCD Type 2 dim_patients_scd successfully loaded to BigQuery.")
