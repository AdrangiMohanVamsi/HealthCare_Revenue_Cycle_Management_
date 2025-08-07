# extractor.py

import pandas as pd
import mysql.connector
from config import db_config

class DataExtractor:
    def __init__(self, db_info):
        self.conn = mysql.connector.connect(
            host=db_info['host'],
            user=db_info['user'],
            password=db_info['password'],
            database=db_info['database']
        )

    def extract_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.conn)

# Example usage
if __name__ == "__main__":
    from pprint import pprint

    extractor_a = DataExtractor(db_config["hospital_a"])
    extractor_b = DataExtractor(db_config["hospital_b"])

    # Extract patients from both
    patients_a = extractor_a.extract_table("patients")
    patients_b = extractor_b.extract_table("patients")

    print("Hospital A Patients Sample:")
    pprint(patients_a.head())

    print("\nHospital B Patients Sample:")
    pprint(patients_b.head())
