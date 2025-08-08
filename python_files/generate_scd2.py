import pandas as pd
from datetime import datetime


patients_df = pd.read_csv("combined_cleaned_patients.csv")
claims_df = pd.read_csv("combined_cleaned_claims.csv")


latest_claims = claims_df.groupby("patient_id", as_index=False)["paid_amount"].sum()
merged_df = pd.merge(patients_df, latest_claims, on="patient_id", how="left")


merged_df["paid_amount"] = merged_df["paid_amount"].fillna(0.0)


merged_df["full_name"] = (
    merged_df["first_name"].fillna("") + " " +
    merged_df["middle_name"].fillna("") + " " +
    merged_df["last_name"].fillna("")
).str.strip()


merged_df["dob"] = pd.to_datetime(merged_df["dob"], errors="coerce")
merged_df["age"] = merged_df["dob"].apply(lambda dob: datetime.now().year - dob.year if pd.notnull(dob) else None)


merged_df.rename(columns={"paid_amount": "insurance"}, inplace=True)


current_date = pd.Timestamp(datetime.now().date())
merged_df["effective_date"] = current_date
merged_df["expiry_date"] = pd.NaT
merged_df["is_current"] = True
merged_df["version"] = 1


scd_df = merged_df[[
    "patient_sk", "patient_id", "first_name", "last_name", "middle_name",
    "phone_number", "gender", "dob", "age", "insurance",
    "effective_date", "expiry_date", "is_current", "version"
]]


scd_df["phone_number"] = scd_df["phone_number"].astype(str)
scd_df["is_current"] = scd_df["is_current"].astype(bool)
scd_df["insurance"] = scd_df["insurance"].astype(float)


scd_df.to_csv("scd_type2_patients.csv", index=False)
print("✅ SCD Type 2 patients CSV created successfully.")
