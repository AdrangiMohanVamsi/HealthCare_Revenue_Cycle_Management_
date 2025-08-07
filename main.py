from google.cloud import bigquery
from google.oauth2 import service_account

# Path to your service account key file
key_path = "C:/Users/vamsi/OneDrive/Desktop/HealthCare_Revenue_Management/gcp_key.json"

# Load credentials from the file
credentials = service_account.Credentials.from_service_account_file(key_path)

# Create BigQuery client with explicit credentials
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Sample query
query = "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 5"
results = client.query(query)

# Print results
for row in results:
    print(row.name)
