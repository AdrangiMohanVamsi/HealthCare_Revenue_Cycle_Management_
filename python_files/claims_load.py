import pandas as pd
from transformer import clean_claims, create_fact_claims, clean_procedure_codes, create_dim_procedures

# --- Load and Combine Claims Data ---
claims1 = pd.read_csv("csv_data/hospital1_claim_data.csv")
claims2 = pd.read_csv("csv_data/hospital2_claim_data.csv")

# Define mapping (if needed)
map_claims = {
    'ClaimID': 'ClaimID', 'TransactionID': 'TransactionID', 'PatientID': 'PatientID',
    'EncounterID': 'EncounterID', 'ProviderID': 'ProviderID', 'DeptID': 'DeptID',
    'ServiceDate': 'ServiceDate', 'ClaimDate': 'ClaimDate', 'PayorID': 'PayorID',
    'ClaimAmount': 'ClaimAmount', 'PaidAmount': 'PaidAmount', 'ClaimStatus': 'ClaimStatus',
    'PayorType': 'PayorType', 'Deductible': 'Deductible', 'Coinsurance': 'Coinsurance',
    'Copay': 'Copay', 'InsertDate': 'InsertDate', 'ModifiedDate': 'ModifiedDate'
}

# Clean claims data
clean_c1 = clean_claims(claims1, map_claims)
clean_c2 = clean_claims(claims2, map_claims)

combined_claims = pd.concat([clean_c1, clean_c2], ignore_index=True)
fact_claims_df = create_fact_claims(combined_claims)

# Save and optional DB load
fact_claims_df.to_csv("combined_cleaned_claims.csv", index=False)
print("✅ fact_claims saved to CSV.")

# --- Load and Clean CPT Procedure Codes ---
cpt_df = pd.read_csv("csv_data/cptcodes.csv")
dim_procedures_df = create_dim_procedures(clean_procedure_codes(cpt_df))

# Save and optional DB load
dim_procedures_df.to_csv("combined_cleaned_procedures.csv", index=False)
print("✅ dim_procedures saved to CSV.")
