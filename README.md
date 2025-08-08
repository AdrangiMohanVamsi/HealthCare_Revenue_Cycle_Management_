# Healthcare Revenue Cycle Management (RCM) Analytics Pipeline


This project is a comprehensive data engineering solution designed to address the challenges of managing healthcare revenue cycles. It involves building a robust ETL pipeline to extract data from disparate hospital systems, process insurance claims, and load the consolidated, cleaned data into a cloud data warehouse for analytics and reporting.

## Table of Contents

- [The Challenge](#the-challenge)
- [Key Features](#key-features)
- [Solution Architecture](#solution-architecture)
- [Analytics Dashboards](#analytics-dashboards)
- [Technical Stack](#technical-stack)
- [Project Structure](#project-structure)


## The Challenge

A regional healthcare network with two hospitals struggled with a fragmented data landscape. Each hospital maintained its own database, making it impossible to perform cross-hospital analysis, track patient data changes historically, or efficiently identify revenue leakage and compliance risks. This project tackles these core problems head-on.

**Business Problems Addressed:**
*   **Data Silos:** Impossible cross-hospital analysis.
*   **Manual Processes:** Delays in claims processing and insights.
*   **No Historical Tracking:** Inability to track changes in patient information over time.
*   **Revenue Leakage:** Difficulty in identifying patterns in claim denials and payment delays.
*   **Compliance Risks:** Lack of clear audit trails for patient data changes.

## Key Features

*   **Unified Data Integration:** Extracts and combines patient, provider, and transaction data from two separate MySQL databases and CSV-based insurance claims.
*   **Data Quality & Governance:** Implements a comprehensive data cleansing, validation, and standardization process to ensure data accuracy and consistency.
*   **Historical Data Tracking (SCD Type 2):** Implements Slowly Changing Dimension (SCD) Type 2 to maintain a full historical record of changes to patient information, crucial for compliance and trend analysis.
*   **Cloud Analytics Warehouse:** Loads the processed data into Google BigQuery, creating a scalable, centralized repository for advanced analytics.
*   **Dimensional Modeling:** Structures the data using a star schema (fact and dimension tables) to optimize analytical queries and enable self-service BI.

## Solution Architecture

The data pipeline follows a standard Extract, Transform, Load (ETL) architecture:

1.  **Extract:**
    *   Patient, provider, and transaction data is extracted from two separate hospital MySQL databases (`hospital-a`, `hospital-b`).
    *   Monthly insurance claim data is ingested from CSV files.
2.  **Transform:**
    *   The raw data is cleaned, standardized (e.g., proper casing for names, standard date formats), and validated.
    *   A unified patient identifier is created to link records across the two hospitals.
    *   Business logic is applied to calculate metrics like patient age and categorize payments.
    *   SCD Type 2 logic is applied to track historical patient data.
    *   The data is modeled into a star schema with fact and dimension tables.
3.  **Load:**
    *   The final, transformed data is loaded into a Google BigQuery dataset (`healthcare_rcm`) for scalable analytics.

## Analytics Dashboards

The following dashboards provide a high-level overview of the key performance indicators for the healthcare network.

| Total Claims vs. Denials                                                                | Insurance Provider Breakdown                                                                   |
| --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| <img src="./Output Screenshots/Total Claims.png" alt="Total Claims" width="450">                            | <img src="./Output Screenshots/Insurance Breakdown.png" alt="Insurance Breakdown" width="450">                   |
| **Patient Encounter History**                                                           | **Fact Table Snippet**                                                                         |
| <img src="./Output Screenshots/Patient History.png" alt="Patient History" width="450">                      | <img src="./Output Screenshots/Fact_Transaction.png" alt="Fact Transaction" width="450">                         |

## Technical Stack

*   **Languages:** `Python`, `SQL`
*   **Databases:** `MySQL`, `Google BigQuery`
*   **Core Libraries:** `pandas`, `sqlalchemy`, `google-cloud-bigquery`
*   **Cloud Platform:** `Google Cloud Platform (GCP)`

## Project Structure

Here is an overview of the key files and directories in this project:

```
HealthCare_Revenue_Management/
├── csv_data/                 # Source CSV files for insurance claims
├── hospital-a/               # Data and DDL for the first hospital
├── hospital-b/               # Data and DDL for the second hospital
├── python_files/             # All Python ETL scripts
│   ├── extractor.py          # Extracts data from sources
│   ├── transformer.py        # Applies cleaning and business logic
│   ├── scd_type2_patients.py # Implements SCD Type 2 logic
│   ├── loader.py             # Loads data into staging
│   └── bigquery_loader.py    # Loads final models into BigQuery
├── cleaned_files/            # Output directory for cleaned, intermediate CSVs
├── documentation/            # Project documentation
├── .gitignore
└── README.md
```

