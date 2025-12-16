# End-to-end Azure Lakehouse Medallion Architecture

📌 Project Overview

This project demonstrates an end-to-end Azure Data Engineering Lakehouse architecture using the Medallion pattern (Bronze, Silver, Gold) on an E-commerce / Retail (AdventureWorks) dataset. The goal is to showcase how raw data is ingested, transformed, curated, and finally served for analytics and reporting.

🏗️ Architecture Overview

High-Level Flow

Data Source – CSV files (AdventureWorks datasets)

Data Ingestion – Azure Data Factory

Raw Storage (Bronze Layer) – Azure Data Lake Gen2

Transformation – Azure Databricks (PySpark)

Curated Storage (Silver Layer) – Azure Data Lake Gen2 (Parquet format)

Serving Layer (Gold) – Azure Synapse Analytics

Reporting – Power BI

<img width="1193" height="687" alt="image" src="https://github.com/user-attachments/assets/7ed4e602-5fbe-4363-9fd9-f234c5553732" />


