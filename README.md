# 🏥 Healthcare Revenue Management using BigQuery & SCD Type 2

This project is a complete end-to-end **Data Engineering solution** for managing healthcare revenue data. It cleans and integrates multiple datasets, applies **Slowly Changing Dimension (SCD) Type 2** logic, and loads the data into **Google BigQuery** across Silver and Gold layers. It also builds **analytical views** for insightful reporting.

---

## 📌 Table of Contents
- [Project Description](#project-description)
- [Features](#features)
- [Architecture](#architecture)
- [Datasets Used](#datasets-used)
- [Tech Stack](#tech-stack)
- [Steps to Run the Project](#steps-to-run-the-project)
- [BigQuery Views (Reporting)](#bigquery-views-reporting)
- [Project Screenshots](#project-screenshots)
- [Author](#author)

---

## 📄 Project Description

This project demonstrates how to design a healthcare data warehouse using Python and Google BigQuery. It handles the integration of various datasets such as claims, patients, and transactions, and performs **SCD Type 2** versioning for patient history. Cleaned and enriched data is structured into **Silver and Gold layers**, and analytical SQL views are created for reporting.

---

## ✅ Features

- 📂 Combine and clean multiple healthcare-related CSVs.
- 🧮 Apply **SCD Type 2** logic to maintain historical patient changes.
- ☁️ Upload cleaned data to **BigQuery Silver Layer**.
- 🪙 Transform to **Gold Layer** with fact and dimension tables.
- 🔍 Create **BigQuery views** for business intelligence and reporting.

---

## 📐 Architecture

```text
Raw CSVs
   ↓
Data Cleaning & Integration (Pandas)
   ↓
SCD Type 2 Handling (dim_patients)
   ↓
BigQuery Silver Layer (cleaned tables)
   ↓
BigQuery Gold Layer (dim & fact tables)
   ↓
SQL Views for Analytics
