# load_claims.py

import pandas as pd

claims_1 = pd.read_csv(r"csv_data/hospital1_claim_data.csv")
claims_2 = pd.read_csv(r"csv_data/hospital2_claim_data.csv")
cpt_codes = pd.read_csv(r"csv_data/cptcodes.csv")

print("Claims 1:")
print(claims_1.head())
