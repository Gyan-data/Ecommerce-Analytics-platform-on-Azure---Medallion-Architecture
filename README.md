# End-to-end Azure Lakehouse Medallion Architecture

**Technologies**: Azure Data Factory | ADLS Gen2 | Azure Databricks | Azure Synapse (Serverless SQL) | Power BI

**Production-grade Azure Lakehouse project demonstrating real-world, enterprise data engineering patterns.**

📌 **Project Overview**

This project demonstrates a production-ready Azure Lakehouse architecture built using the Medallion pattern (Bronze → Silver → Gold) on an E-commerce / Retail (AdventureWorks) dataset.

The objective is to showcase real-world Azure Data Engineering skills, including:

- Metadata-driven ingestion(No Hard coding)

- Secure cloud authentication (Entra ID, Managed Identity)

- Scalable Spark transformations

- Lakehouse analytics using Synapse Serverless SQL

- Business reporting with Power BI

## Architecture Diagram

- High-Level Flow

Data Source → ADF → ADLS (Bronze) → Databricks (Silver) → Synapse (Gold) → Power BI

<img src="https://github.com/user-attachments/assets/7ed4e602-5fbe-4363-9fd9-f234c5553732" />

🧰 Technology Stack

- Layer	Technology

- Ingestion	 → Azure Data Factory

- Storage	Azure  → Data Lake Gen2

- Transformation  →	Azure Databricks (PySpark)

- Serving	 → Azure Synapse Analytics (Serverless SQL)
  
- Reporting  → 	Power BI

- Security	 → Azure Entra ID, Managed Identity

Dataset

Source: AdventureWorks Dataset (Kaggle)

Link: https://www.kaggle.com/datasets/ukveteran/adventure-works/data

Files Used:

- Customers

- Products

- Categories

- Sales (2015–2017)

- Returns

- Calendar

- Territories

All files are stored under the /Data directory.

🟤 Bronze Layer – Data Ingestion (Azure Data Factory)

🎯 Objective

 Ingest multiple CSV files from GitHub into ADLS dynamically, without hardcoding file paths or names.

❌ Why Not Static Pipelines?

1. One Copy Activity per file

2. Hardcoded paths & filenames

3. Poor scalability & maintenance

✅ Dynamic Metadata-Driven Design

ADF reads ingestion instructions from a JSON control file (git.json) stored in ADLS.

Each entry defines:

- Source -> file path

- Target -> folder

- Output -> filename

{
  "relative_url": "Gyan-data/.../AdventureWorks_Customers.csv",
  "raw_folder": "AdventureWorks_Customers",
  "file_name": "AdventureWorks_Customers.csv"
}


👉 New files can be ingested by updating JSON only. No pipeline changes required.

🔄 ADF Pipeline Flow

- Lookup Activity – Reads git.json

- ForEach Activity – Iterates dynamically

- Copy Data Activity – GitHub → ADLS Bronze (parameterized)

✔ Scalable

✔ Reusable 

✔ Enterprise-ready

<img src="https://github.com/user-attachments/assets/d711ec46-30de-434d-9857-39ed488462a6" /> <img src="https://github.com/user-attachments/assets/18745db4-7ba2-44f0-a54f-712309cde0e5" />

🔐 Secure Databricks ↔ ADLS Authentication (Azure Entra ID) Authentication Model

- Service Principal (OAuth)

- No storage keys

- No secrets in code

- Security Steps Implemented

- Azure App Registration

- Client ID & Tenant ID

- Client Secret (stored securely)

RBAC: Storage Blob Data Contributor

Databricks Secret Scope

OAuth Spark configuration

spark.conf.set( "fs.azure.account.oauth2.client.id", dbutils.secrets.get(scope="adls-scope", key="client-id") )

✔ Secure

✔ Production-grade 

✔ Audit-friendly

⚙️ Silver Layer – Data Transformation (Azure Databricks)

**Transformation Principles**

- Read from Bronze

- Apply business logic

- Use PySpark DataFrame APIs

- Write optimized Parquet data

🔹 Key Transformations

- Calendar

- Extract Month & Year

- Customers

- Create FullName using concat_ws()

- Products

- SKU & Name normalization using split()

- Sales (Largest Dataset)

- Date → Timestamp

- String standardization

- Derived metrics

df_sales = df_sales.withColumn(
'multiply', col('OrderLineItem') * col('OrderQuantity')
)

<img src="https://github.com/user-attachments/assets/ca6efb36-90e1-4c96-98a0-832215801f6d" />

📊 Databricks Analysis Example


Business Question:

How many orders are placed per day?

df_sales.groupBy("OrderDate") \
.agg(count("OrderNumber").alias("TotalOrders"))


Visualizations created directly in Databricks for quick insights.

🟡 Gold Layer – Azure Synapse Analytics (Lakehouse)

Why Synapse?

ADF + Spark + SQL Analytics = Synapse

<img src="https://github.com/user-attachments/assets/13fd0318-bb68-4888-9763-ee5e2b382528" />

🔑 Security Model

- Uses System Assigned Managed Identity

- No secrets or passwords

- RBAC-based access to ADLS

“Synapse accesses ADLS using Managed Identity”

🏗️ Why Lakehouse Architecture

Data Lake: Low-cost storage but limited analytics capability

Data Warehouse: Strong SQL and BI support but expensive and less scalable

Lakehouse: Combines cheap storage + SQL analytics + BI friendliness + scalability, making it the modern preferred architecture

- Serverless SQL (Key Concept)

- No infrastructure to manage

- Pay-per-query

Uses OPENROWSET() to read Parquet directly from ADLS

SELECT *
FROM OPENROWSET(
    BULK 'silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS result

Gold Layer Design

Views → Logical business layer

External Tables (CETAS) → Physical Gold data in ADLS

✔ Stable schemas

✔ Power BI optimized

✔ Enterprise governance

📈 Power BI – Reporting Layer

Connectivity

- Connects to Synapse Serverless SQL Endpoint

- Reads Gold views / external tables

- No direct ADLS access required

<img src="https://github.com/user-attachments/assets/b6357724-6f14-482b-9393-f230afc1c9f8" /> <img src="https://github.com/user-attachments/assets/e67efb94-70df-414e-9a38-ff8855a00733" />

- Sample Insights

- Total Customers

- Year-wise growth

- Sales trends

🧩 **Business Use Cases Enabled**

- Daily sales and revenue reporting

- Customer growth analysis

- Product performance insights

- Year-over-year trend analysis

- Self-service BI reporting

## Key Concepts Demonstrated

- Metadata-driven ETL design
  
- Secure Azure authentication (Entra ID, Managed Identity)
  
- Spark optimization & partitioned writes
  
- Lakehouse architecture with Serverless SQL
  
- BI-ready Gold layer design


**👤 Author**

**Gyan Singh**

Data Engineer

Azure | AWS | ADF | Databricks | PySpark | SQL





   





