import pandas as pd
import re
from datetime import datetime


def calculate_age(dob):
    if pd.isnull(dob):
        return None
    today = datetime.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def clean_patients(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    df['FirstName'] = df['FirstName'].str.title()
    df['LastName'] = df['LastName'].str.title()
    df['PhoneNumber'] = df['PhoneNumber'].astype(str).apply(lambda x: re.sub(r'\D', '', x))
    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')
    df['Age'] = df['DOB'].apply(calculate_age)
    return df


def clean_providers(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    df['FullName'] = df['FirstName'].str.title() + ' ' + df['LastName'].str.title()
    return df[['ProviderID', 'FullName', 'Specialization', 'DeptID', 'NPI']]


def clean_transactions(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    date_fields = ['VisitDate', 'ServiceDate', 'PaidDate', 'InsertDate', 'ModifiedDate']
    for col in date_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def clean_departments(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    df['Name'] = df['Name'].str.title()
    return df


def clean_encounters(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    date_fields = ['EncounterDate', 'InsertedDate', 'ModifiedDate']
    for col in date_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def create_dim_patients(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    if 'patient_sk' not in df.columns:
        df.insert(0, 'patient_sk', df.index + 1)

    return df.rename(columns={
        'PatientID': 'patient_id',
        'FirstName': 'first_name',
        'LastName': 'last_name',
        'MiddleName': 'middle_name',
        'PhoneNumber': 'phone_number',
        'Gender': 'gender',
        'DOB': 'dob',
        'Age': 'age'
    })[['patient_sk', 'patient_id', 'first_name', 'last_name',
        'middle_name', 'phone_number', 'gender', 'dob', 'age']]


def create_dim_providers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    if 'provider_sk' not in df.columns:
        df.insert(0, 'provider_sk', df.index + 1)

    return df.rename(columns={
        'ProviderID': 'provider_id',
        'FullName': 'full_name',
        'Specialization': 'specialization',
        'DeptID': 'dept_id',
        'NPI': 'npi'
    })[['provider_sk', 'provider_id', 'full_name', 'specialization', 'dept_id', 'npi']]


def create_dim_departments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    if 'department_sk' not in df.columns:
        df.insert(0, 'department_sk', df.index + 1)

    return df.rename(columns={
        'DeptID': 'dept_id',
        'Name': 'dept_name'
    })[['department_sk', 'dept_id', 'dept_name']]


def create_dim_encounters(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    if 'encounter_sk' not in df.columns:
        df.insert(0, 'encounter_sk', df.index + 1)

    return df.rename(columns={
        'EncounterID': 'encounter_id',
        'PatientID': 'patient_id',
        'ProviderID': 'provider_id',
        'DepartmentID': 'dept_id',
        'EncounterType': 'encounter_type',
        'EncounterDate': 'encounter_date',
        'ProcedureCode': 'procedure_code',
        'InsertedDate': 'insert_date',
        'ModifiedDate': 'modified_date'
    })[['encounter_sk', 'encounter_id', 'patient_id', 'provider_id',
        'dept_id', 'encounter_type', 'encounter_date',
        'procedure_code', 'insert_date', 'modified_date']]


def create_dim_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)

    
    if 'transaction_sk' not in df.columns:
        df.insert(0, 'transaction_sk', df.index + 1)

    df = df.rename(columns={
        'TransactionID': 'transaction_id',
        'EncounterID': 'encounter_id',
        'PatientID': 'patient_id',
        'ProviderID': 'provider_id',
        'DeptID': 'dept_id',
        'VisitDate': 'visit_date',
        'ServiceDate': 'service_date',
        'PaidDate': 'paid_date',
        'VisitType': 'visit_type',
        'Amount': 'amount',
        'AmountType': 'amount_type',
        'PaidAmount': 'paid_amount',
        'ClaimID': 'claim_id',
        'PayorID': 'payor_id',
        'ProcedureCode': 'procedure_code',
        'ICDCode': 'icd_code',
        'LineOfBusiness': 'line_of_business',
        'MedicaidID': 'medicaid_id',
        'MedicareID': 'medicare_id',
        'InsertDate': 'insert_date',
        'ModifiedDate': 'modified_date'
    })

    
    columns = ['transaction_sk', 'transaction_id', 'encounter_id', 'patient_id', 'provider_id',
               'dept_id', 'visit_date', 'service_date', 'paid_date', 'visit_type', 'amount',
               'amount_type', 'paid_amount', 'claim_id', 'payor_id', 'procedure_code', 'icd_code',
               'line_of_business', 'medicaid_id', 'medicare_id', 'insert_date', 'modified_date']

    
    return df[[col for col in columns if col in df.columns]]
import pandas as pd
import re
from datetime import datetime


def clean_claims(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    df = df.rename(columns=column_map)
    
    date_cols = ['ServiceDate', 'ClaimDate', 'InsertDate', 'ModifiedDate']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    
    for col in ['ClaimAmount', 'PaidAmount', 'Deductible', 'Coinsurance', 'Copay']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def create_fact_claims(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)

    
    if 'claim_sk' not in df.columns:
        df.insert(0, 'claim_sk', df.index + 1)

    df = df.rename(columns={
        'ClaimID': 'claim_id',
        'TransactionID': 'transaction_id',
        'PatientID': 'patient_id',
        'EncounterID': 'encounter_id',
        'ProviderID': 'provider_id',
        'DeptID': 'dept_id',
        'ServiceDate': 'service_date',
        'ClaimDate': 'claim_date',
        'PayorID': 'payor_id',
        'ClaimAmount': 'claim_amount',
        'PaidAmount': 'paid_amount',
        'ClaimStatus': 'claim_status',
        'PayorType': 'payor_type',
        'Deductible': 'deductible',
        'Coinsurance': 'coinsurance',
        'Copay': 'copay',
        'InsertDate': 'insert_date',
        'ModifiedDate': 'modified_date'
    })

    return df[['claim_sk', 'claim_id', 'transaction_id', 'patient_id', 'encounter_id',
               'provider_id', 'dept_id', 'service_date', 'claim_date',
               'payor_id', 'claim_amount', 'paid_amount', 'claim_status',
               'payor_type', 'deductible', 'coinsurance', 'copay',
               'insert_date', 'modified_date']]


def clean_procedure_codes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        'Procedure Code Category': 'category',
        'CPT Codes': 'cpt_code',
        'Procedure Code Descriptions': 'description',
        'Code Status': 'status'
    })
    df = df.dropna(subset=['cpt_code'])  
    df['cpt_code'] = df['cpt_code'].astype(str).str.strip()
    return df


def create_dim_procedures(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    df.insert(0, 'procedure_sk', df.index + 1)
    return df[['procedure_sk', 'cpt_code', 'description', 'category', 'status']]
