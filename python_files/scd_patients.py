import pandas as pd
import os
from datetime import datetime


new_df = pd.read_csv("combined_cleaned_patients.csv")
new_df['effective_date'] = pd.to_datetime(datetime.today().date())
new_df['expiry_date'] = pd.NaT
new_df['is_current'] = True
new_df['version'] = 1

scd_file = "dim_patients_scd.csv"
if os.path.exists(scd_file):
    old_df = pd.read_csv(scd_file, parse_dates=['effective_date', 'expiry_date'])
    final_scd = []

    for _, new_row in new_df.iterrows():
        existing = old_df[(old_df['patient_id'] == new_row['patient_id']) & (old_df['is_current'] == True)]

        if existing.empty:
            final_scd.append(new_row)
        else:
            existing_row = existing.iloc[0]
            changes = False

            for col in ['first_name', 'last_name', 'phone_number', 'gender', 'dob']:
                if pd.notna(new_row[col]) and new_row[col] != existing_row[col]:
                    changes = True
                    break

            if changes:
                
                old_df.loc[existing.index, 'expiry_date'] = new_row['effective_date']
                old_df.loc[existing.index, 'is_current'] = False

                
                new_row['version'] = existing_row['version'] + 1
                final_scd.append(new_row)
            else:
                
                continue

    updated_df = pd.concat([old_df, pd.DataFrame(final_scd)], ignore_index=True)
else:
    updated_df = new_df


updated_df = updated_df.sort_values(by=['patient_id', 'version'])


if 'patient_sk' in updated_df.columns:
    updated_df = updated_df.drop(columns=['patient_sk'])


updated_df.insert(0, 'patient_sk', range(1, len(updated_df)+1))

updated_df.to_csv(scd_file, index=False)

print("✅ dim_patients_scd.csv updated successfully.")
