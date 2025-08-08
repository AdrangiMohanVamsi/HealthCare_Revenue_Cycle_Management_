import pandas as pd
from datetime import datetime
import os

def apply_scd_type2(existing_df, new_df):
    scd_cols = ['full_name', 'gender', 'dob', 'address', 'phone', 'email', 'insurance_provider']
    current_date = pd.Timestamp(datetime.now().date())

    output_df = existing_df.copy()

    for _, new_row in new_df.iterrows():
        pid = new_row['patient_id']
        existing_rows = existing_df[existing_df['patient_id'] == pid]

        if existing_rows.empty:
            
            new_entry = new_row.to_dict()
            new_entry.update({
                'effective_date': current_date,
                'expiry_date': pd.NaT,
                'is_current': True,
                'version': 1
            })
            output_df = pd.concat([output_df, pd.DataFrame([new_entry])], ignore_index=True)
        else:
            
            current_record = existing_rows[existing_rows['is_current'] == True].iloc[0]
            has_changed = any(new_row[col] != current_record[col] for col in scd_cols)

            if has_changed:
                
                output_df.loc[(output_df['patient_id'] == pid) & (output_df['is_current']), 'expiry_date'] = current_date
                output_df.loc[(output_df['patient_id'] == pid) & (output_df['is_current']), 'is_current'] = False

                
                new_entry = new_row.to_dict()
                new_entry.update({
                    'effective_date': current_date,
                    'expiry_date': pd.NaT,
                    'is_current': True,
                    'version': current_record['version'] + 1
                })
                output_df = pd.concat([output_df, pd.DataFrame([new_entry])], ignore_index=True)

    return output_df



new_df = pd.read_csv("combined_cleaned_patients.csv")


if os.path.exists("scd_type2_patients.csv"):
    existing_df = pd.read_csv("scd_type2_patients.csv", parse_dates=['effective_date', 'expiry_date'])
else:
    
    existing_df = pd.DataFrame(columns=new_df.columns.tolist() + ['effective_date', 'expiry_date', 'is_current', 'version'])


final_scd_df = apply_scd_type2(existing_df, new_df)


final_scd_df.to_csv("scd_type2_patients.csv", index=False)
print("✅ scd_type2_patients.csv generated successfully.")
