import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
from config import db_config
from transformer import (
    clean_patients, create_dim_patients,
    clean_providers, create_dim_providers,
    clean_transactions, clean_departments, clean_encounters,
    create_dim_departments,create_dim_encounters,create_dim_transactions
)


mysql_config = db_config['hospital_a']
encoded_password = urllib.parse.quote_plus(mysql_config['password'])

engine = create_engine(
    f"mysql+mysqlconnector://{mysql_config['user']}:{encoded_password}@{mysql_config['host']}/{mysql_config['database']}"
)


patients_a = pd.read_csv("hospital-a/patients.csv")
patients_b = pd.read_csv("hospital-b/patients.csv")

map_patients_a = {
    'PatientID': 'PatientID', 'FirstName': 'FirstName', 'LastName': 'LastName',
    'MiddleName': 'MiddleName', 'SSN': 'SSN', 'PhoneNumber': 'PhoneNumber',
    'Gender': 'Gender', 'DOB': 'DOB', 'Address': 'Address', 'ModifiedDate': 'ModifiedDate'
}
map_patients_b = {
    'ID': 'PatientID', 'F_Name': 'FirstName', 'L_Name': 'LastName',
    'M_Name': 'MiddleName', 'SSN': 'SSN', 'PhoneNumber': 'PhoneNumber',
    'Gender': 'Gender', 'DOB': 'DOB', 'Address': 'Address', 'Updated_Date': 'ModifiedDate'
}

clean_pa = clean_patients(patients_a, map_patients_a)
clean_pb = clean_patients(patients_b, map_patients_b)
combined_patients = pd.concat([clean_pa, clean_pb], ignore_index=True)
dim_patients_df = create_dim_patients(combined_patients)
dim_patients_df.to_csv("combined_cleaned_patients.csv", index=False)
dim_patients_df.to_sql('dim_patients', con=engine, if_exists='replace', index=False)
print("✅ dim_patients loaded.")


providers_a = pd.read_csv("hospital-a/providers.csv")
providers_b = pd.read_csv("hospital-b/providers.csv")

map_providers = {
    'ProviderID': 'ProviderID', 'FirstName': 'FirstName', 'LastName': 'LastName',
    'Specialization': 'Specialization', 'DeptID': 'DeptID', 'NPI': 'NPI'
}

clean_pa = clean_providers(providers_a, map_providers)
clean_pb = clean_providers(providers_b, map_providers)
combined_providers = pd.concat([clean_pa, clean_pb], ignore_index=True)
dim_providers_df = create_dim_providers(combined_providers)
dim_providers_df.to_csv("combined_cleaned_providers.csv", index=False)
dim_providers_df.to_sql('dim_providers', con=engine, if_exists='replace', index=False)
print("✅ dim_providers loaded.")


transactions_a = pd.read_csv("hospital-a/transactions.csv")
transactions_b = pd.read_csv("hospital-b/transactions.csv")

map_transactions = {
    'TransactionID': 'TransactionID', 'EncounterID': 'EncounterID',
    'PatientID': 'PatientID', 'ProviderID': 'ProviderID', 'DeptID': 'DeptID',
    'VisitDate': 'VisitDate', 'ServiceDate': 'ServiceDate', 'PaidDate': 'PaidDate',
    'VisitType': 'VisitType', 'Amount': 'Amount', 'AmountType': 'AmountType',
    'PaidAmount': 'PaidAmount', 'ClaimID': 'ClaimID', 'PayorID': 'PayorID',
    'ProcedureCode': 'ProcedureCode', 'ICDCode': 'ICDCode', 'LineOfBusiness': 'LineOfBusiness',
    'MedicaidID': 'MedicaidID', 'MedicareID': 'MedicareID',
    'InsertDate': 'InsertDate', 'ModifiedDate': 'ModifiedDate'
}

clean_ta = clean_transactions(transactions_a, map_transactions)
clean_tb = clean_transactions(transactions_b, map_transactions)
combined_transactions = pd.concat([clean_ta, clean_tb], ignore_index=True)
dim_transactions_df = create_dim_transactions(combined_transactions)
dim_transactions_df.to_csv("combined_cleaned_transactions.csv", index=False)
dim_transactions_df.to_sql('dim_transactions', con=engine, if_exists='replace', index=False)
print("✅ dim_transactions loaded.")

departments_a = pd.read_csv('hospital-a/departments.csv')
departments_b = pd.read_csv('hospital-b/departments.csv')
map_departments = {'DeptID':'DeptID', 'Name':'Name'}
clean_da = clean_departments(departments_a, map_departments)
clean_db = clean_departments(departments_b,map_departments)

combined_departments = pd.concat([clean_da,clean_db], ignore_index=False)
dim_departments_df = create_dim_departments(combined_departments)
dim_departments_df.to_csv("combined_cleaned_departments.csv", index=False)
dim_departments_df.to_sql('dim_departments', con=engine, if_exists='replace', index=False)
print("✅ dim_departments loaded.")

encounters_a = pd.read_csv("hospital-a/encounters.csv")
encounters_b = pd.read_csv("hospital-b/encounters.csv")

map_encounters = {
    'EncounterID':'EncounterID','PatientID':'PatientID',
    'EncounterDate':'EncounterDate','EncounterType':'EncounterType',
    'ProviderID':'ProviderID','DepartmentID':'DepartmentID',
    'ProcedureCode':'ProcedureCode','InsertedDate':'InsertedDate','ModifiedDate':'ModifiedDate'
}

clean_ea = clean_encounters(encounters_a, map_encounters)
clean_eb = clean_encounters(encounters_b, map_encounters)
combined_encounters = pd.concat([clean_ea, clean_eb], ignore_index=True)
dim_encounters_df = create_dim_encounters(combined_encounters)
dim_encounters_df.to_csv("combined_cleaned_encounters.csv", index=False)
dim_encounters_df.to_sql('dim_encounters', con=engine, if_exists='replace', index=False)
print("✅ dim_encounters loaded.")
