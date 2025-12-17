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

Technology Stack

Azure Data Factory (ADF) – Data ingestion & orchestration

Azure Data Lake Storage Gen2 (ADLS) – Central data lake

Azure Databricks – Data transformation (Spark)

Azure Synapse Analytics – Serving & analytics layer

Power BI – Reporting & dashboards

Azure Entra ID (Service Principal) – Secure access management

GitHub – Version control

Dataset

Source: AdventureWorks Dataset (Kaggle)

Link: https://www.kaggle.com/datasets/ukveteran/adventure-works/data

Files used:

AdventureWorks_Calendar.csv

AdventureWorks_Customers.csv

AdventureWorks_Products.csv

AdventureWorks_Product_Categories.csv

AdventureWorks_Product_Subcategories.csv

AdventureWorks_Sales_2015.csv

AdventureWorks_Sales_2016.csv

AdventureWorks_Sales_2017.csv

AdventureWorks_Returns.csv

AdventureWorks_Territories.csv

All source files are stored under the Data/ directory in this repository.

Data Ingestion – Azure Data Factory (Bronze Layer)
Objective

The goal of the ingestion layer is to reliably and scalably ingest multiple source files from GitHub into Azure Data Lake Gen2 (Bronze layer) without creating repetitive or hardcoded pipelines.

Why Not a Static Approach?

In a static pipeline design:

Each file requires a separate Copy Activity

Folder paths and file names are hardcoded

Any new file requires pipeline modification

This approach is:

Not scalable

Difficult to maintain

Error-prone in enterprise environments

Therefore, this project uses a dynamic, metadata-driven ingestion approach.

Dynamic Ingestion Design (Metadata-Driven)

Instead of hardcoding values, the pipeline reads all variable information from a JSON configuration file stored in ADLS.

This JSON acts as a control file that drives the entire ingestion process.

JSON Control File (git.json)

The git.json file is stored inside a dedicated parameters container in ADLS.

Each JSON object represents one file ingestion instruction.

Example structure:

{
  "relative_url": "Gyan-data/Ecommerce-Azure-Lakehouse-Medallion-Project/refs/heads/main/Data/AdventureWorks_Customers.csv",
  
  "raw_folder": "AdventureWorks_Customers",
  
  "file_name": "AdventureWorks_Customers.csv"
}
Meaning of Each Field

relative_url: Relative GitHub path of the source file

raw_folder: Target folder name inside the Bronze container

file_name: Output file name in ADLS

This design allows adding a new file by only updating the JSON, without touching the pipeline.

Azure Data Factory Pipeline Flow

1. Lookup Activity – Lookup_git

Reads git.json from ADLS

Returns an array of file metadata

Output becomes the input for iteration

2. ForEach Activity – ForEach_dynamic

Iterates over each JSON object

Enables parallel and scalable ingestion

Expression used:

@activity('Lookup_git').output.value

Each iteration processes one file dynamically.

3. Copy Data Activity (Inside ForEach)
Source

HTTP / GitHub raw file endpoint

Base URL remains constant

relative_url changes dynamically per iteration

Sink

Azure Data Lake Gen2 (Bronze container)

Folder and file names are parameterized

Dataset parameters:

raw_folder → @item().raw_folder

file_name → @item().file_name

relative_url → @item().relative_url

This ensures:

Clean folder separation

No hardcoding

Fully reusable pipeline

<img width="1912" height="873" alt="image" src="https://github.com/user-attachments/assets/d711ec46-30de-434d-9857-39ed488462a6" />

<img width="1920" height="597" alt="image" src="https://github.com/user-attachments/assets/18745db4-7ba2-44f0-a54f-712309cde0e5" />

Databricks ↔ ADLS Connection using Entra ID

Step 1: Create App Registration (Service Principal)

Go to Azure Portal → Microsoft Entra ID → App registrations

Click New registration

Enter:

Name: Data_project

Supported account type: Single tenant

Click Register

After creation, note the following values:

Application (Client) ID

Directory (Tenant) ID

Step 2: Create Client Secret

Open the registered application

Go to Certificates & secrets

Click New client secret

Add description and expiry

Copy the Client Secret Value (shown only once)

These three values are mandatory:

Tenant ID

Client ID

Client Secret

Step 3: Assign RBAC Role on ADLS

Open Azure Data Lake Storage account

Go to Access Control (IAM)

Click Add → Role assignment

Select role:

Storage Blob Data Contributor

Assign access to:

User, group, or service principal

Select the created application (Data_project)

Save

This grants Databricks permission to read and write data in ADLS.

Step 4: Store Secrets Securely in Databricks

In Azure Databricks:

Create a Secret Scope (backed by Databricks)

Store secrets:

tenant-id

client-id

client-secret

This ensures:

Secrets are encrypted

Not exposed in notebooks

Step 5: Configure Databricks to Access ADLS

Use the following Spark configuration once per cluster or notebook:

configs = {
"fs.azure.account.auth.type": "OAuth",

"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",

"fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="adls-scope", key="client-id"),

"fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adls-scope", key="client-secret"),

"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"
}
Replace <TENANT_ID> with your Directory (Tenant) ID.

for key, value in configs.items():

spark.conf.set(key, value)


for key, value in configs.items():

spark.conf.set(key, value)

Step 6: Read Data from ADLS in Databricks

df_cus = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("abfss://bronze@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Customers")
)

display(df_cus)

<img width="1920" height="918" alt="image" src="https://github.com/user-attachments/assets/17df2c4f-b92f-4947-8a0f-9b43fb1decad" />
<img width="1920" height="503" alt="image" src="https://github.com/user-attachments/assets/3b4c1caf-5036-42a3-9080-408b842d24c5" />
<img width="1411" height="653" alt="image" src="https://github.com/user-attachments/assets/7565622f-b030-48df-a73c-647ba22a703d" />

Authentication Flow (Conceptual)

Databricks requests access to ADLS

Entra ID validates Service Principal credentials

OAuth token is issued

ADLS authorizes access based on RBAC

Databricks reads/writes data securely

Key Security Benefits

No storage keys exposed

Centralized identity management

RBAC-controlled access

Production-ready architecture

Transformation Layer – Azure Databricks (Silver Layer)

This section explains all transformations performed in Azure Databricks, exactly as implemented in the notebooks. The objective of the Silver layer is to clean, enrich, standardize, and prepare data coming from the Bronze layer so that it can be used for analytics and reporting.

All transformations are written using PySpark DataFrame APIs, which are industry standard for large-scale data processing.

General Transformation Principles

Read raw data from Bronze (ADLS Gen2)

Apply business-friendly transformations

Avoid hardcoding logic

Use reusable and scalable PySpark functions

Write transformed data to Silver layer in Parquet format

Write modes available in PySpark:

append – Add new data (used in this project)

overwrite – Replace existing data

error – Fail if data exists

ignore – Skip write if data exists

1. Calendar Transformation
Input

Bronze table contains only one column: Date

Business Requirement

Enable reporting by Month and Year

Support use cases like:

Monthly sales

Yearly trends

Transformation Logic

We derive two new columns from the Date column:

Month

Year

PySpark Functions Used

withColumn() – Create or modify columns

month() – Extract month from date

year() – Extract year from date

Note: Column names must be passed in single quotes when using col().

Code
from pyspark.sql.functions import *

from pyspark.sql.types import *


# Create Month and Year columns
df_cal = df.withColumn('Month', month(col('Date'))) \
.withColumn('Year', year(col('Date')))

Write to Silver Layer

df_cal.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Calendar') \
.save()

2. Customers Transformation
Input

Customer personal details from Bronze layer

Business Requirement

Create a FullName column for reporting and analytics

Approach 1 – Basic Concatenation (Learning Purpose)

Functions used:

concat()

lit() – To add constant values like spaces

df_cus = df_cus.withColumn(
'Fullname',
concat(
col('Prefix'),
lit(' '),
col('FirstName'),
lit(' '),
col('LastName')
)
)

Limitation

Repeated use of lit(' ') makes code verbose

Approach 2 – Optimized & Recommended

We use concat_ws() (concatenate with separator), which is cleaner and more scalable.

df_cus = df_cus.withColumn(
'FullName',
concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName'))
)

Write to Silver Layer

df_cus.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Customers') \
.save()

3. Product Subcategories
Observation

Data is already clean

No transformation required

Action

Directly move data from Bronze to Silver

df_prdt_sub.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Subcategories') \
.save()

4. Products Transformation
Input Columns of Interest

ProductSKU

ProductName

Business Requirements

Extract alphabet code before hyphen (-) from ProductSKU

Example: HL-U509-R → HL

Extract first word from ProductName

Example: Sport-100 Helmet Red → Sport-100

Transformation Concept

We use the split() function:

Splits a column into an array

Indexing is applied to fetch required element

PySpark Functions Used

split()

withColumn()

Code

df_product = df_product \
.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0]) \
.withColumn('ProductName', split(col('ProductName'), ' ')[0])

Write to Silver Layer

df_product.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Products') \
.save()

5. Returns Data
Observation

Data structure is simple

No transformation required

Action

Load directly into Silver layer

df_ret.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Returns') \
.save()

6. Territories Data
Input

Sales territory data containing:

SalesTerritoryKey

Region

Country

Continent

Observation

Data is already standardized and analytics-ready

No enrichment or cleansing is required

Action

Directly move data from Bronze to Silver layer

df_territory.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Territories') \
.save()

7. Sales Data Transformation
Dataset Characteristics

Largest dataset in the project

Used for aggregations and analytical reporting

Business Requirements

Convert StockDate from date to timestamp

Replace alphabet S with T in OrderNumber

Create a derived column by multiplying OrderLineItem and OrderQuantity

7.1 Date to Timestamp Conversion
Reason

Timestamp is required for advanced time-based analytics

Enables hourly/minute-level analysis in future

PySpark Function Used

to_timestamp()

df_sales = df_sales.withColumn(
'StockDate',
to_timestamp(col('StockDate'))
)

7.2 String Replacement in OrderNumber
Requirement

Replace S with T to standardize order numbering

PySpark Function Used

regexp_replace()

df_sales = df_sales.withColumn(
'OrderNumber',
regexp_replace(col('OrderNumber'), 'S', 'T')
)

7.3 Mathematical Transformation
Requirement

Calculate a derived metric by multiplying:

OrderLineItem

OrderQuantity

PySpark Function Used

Column arithmetic

df_sales = df_sales.withColumn(
'multiply',
col('OrderLineItem') * col('OrderQuantity')
)

Write Sales Data to Silver Layer

df_sales.write.format('parquet') \
.mode('append') \
.option('path', 'abfss://silver@azuredatalakestorage09.dfs.core.windows.net/AdventureWorks_Sales') \
.save()

8. Sales Analysis (Databricks)
Business Question

How many orders are received each day?

Approach

Group data by OrderDate

Count total number of orders per day

PySpark Functions Used

groupBy()

count()

alias()

df_sales.groupBy('OrderDate') \
.agg(count('OrderNumber').alias('TotalOrders')) \
.display()

<img width="1474" height="724" alt="image" src="https://github.com/user-attachments/assets/ca6efb36-90e1-4c96-98a0-832215801f6d" />

<img width="1429" height="696" alt="image" src="https://github.com/user-attachments/assets/e7a76b50-6e6e-49db-9d7c-3fab0b4c024d" />

<img width="1430" height="712" alt="image" src="https://github.com/user-attachments/assets/79e58a5f-c0f2-49b5-8502-e3c56ce2110c" />

<img width="1410" height="615" alt="image" src="https://github.com/user-attachments/assets/550575af-f07a-439b-9b8a-664c3166e6c3" />

<img width="1416" height="678" alt="image" src="https://github.com/user-attachments/assets/81142f78-86e4-4f97-943e-da0e3d8d6393" />

<img width="1447" height="759" alt="image" src="https://github.com/user-attachments/assets/20a97867-2274-4281-b305-e14d7fe44f1b" />

<img width="1419" height="763" alt="image" src="https://github.com/user-attachments/assets/fff0e798-7768-4e7e-b37e-9e03d7aaeec4" />

<img width="1444" height="737" alt="image" src="https://github.com/user-attachments/assets/aec5227d-59ec-4b79-a2fa-0c2774f65621" />

<img width="1395" height="705" alt="image" src="https://github.com/user-attachments/assets/f394fdec-589b-4fb0-8067-237761bd82be" />

<img width="1438" height="761" alt="image" src="https://github.com/user-attachments/assets/a80484f5-bba2-460c-90d0-90becc3398aa" />

<img width="1273" height="758" alt="image" src="https://github.com/user-attachments/assets/399f9be1-3dd3-4b32-9309-14207df0f9d8" />

<img width="1500" height="743" alt="image" src="https://github.com/user-attachments/assets/afb62672-9c0e-4e08-9c56-c1d615d84944" />

<img width="1453" height="690" alt="image" src="https://github.com/user-attachments/assets/a7526ef0-c6ec-4cef-b212-e1e393bb3837" />

<img width="1476" height="721" alt="image" src="https://github.com/user-attachments/assets/fc06e024-c0f6-47a9-aa2a-ba2c805fb8ed" />

<img width="1508" height="778" alt="image" src="https://github.com/user-attachments/assets/a754d80d-3371-4ec9-9e9b-42def88c6d45" />

<img width="1454" height="671" alt="image" src="https://github.com/user-attachments/assets/1a5bb973-abe7-4f6f-8665-ae786966f3c4" />

<img width="1465" height="695" alt="image" src="https://github.com/user-attachments/assets/73c3844d-041b-49e2-9a94-613dbc7991be" />

<img width="1428" height="666" alt="image" src="https://github.com/user-attachments/assets/874c1f4c-bfbf-4925-bd08-5de49f3cb554" />

<img width="1412" height="703" alt="image" src="https://github.com/user-attachments/assets/c968dc20-77ae-4f72-afe7-dca45cb8b3af" />

<img width="1406" height="766" alt="image" src="https://github.com/user-attachments/assets/75bff9ab-93da-49d1-9ec1-bceda2dd6d68" />

<img width="1469" height="710" alt="image" src="https://github.com/user-attachments/assets/51c2b3a3-e3cd-46d6-8ee5-613e7e66d6cd" />

<img width="1416" height="730" alt="image" src="https://github.com/user-attachments/assets/323897b5-792d-4571-b2a3-e205a4502f7b" />


9. Visualization in Databricks
Purpose

Tables make trend analysis difficult

Visual charts provide better insights for stakeholders

Steps Followed

Click on table dropdown

Click on + icon

Select Visualization

Choose appropriate chart type

Outcome

Daily sales trends are clearly visible

Can be directly shared with managers and stakeholders

Key Takeaways from Transformation Layer

Silver layer contains clean, enriched, and analytics-ready data

Transformations are business-driven

PySpark functions used are interview-relevant

Large datasets are optimized for aggregation

Parquet format improves query performance

Silver layer contains cleaned and enriched data

Transformations are business-driven, not random

PySpark functions used are interview-relevant

Code follows enterprise data engineering standards

Data is stored in columnar Parquet format for performance
**
###AZURE SYNAPES ANALYTICS**

<img width="1920" height="798" alt="image" src="https://github.com/user-attachments/assets/13fd0318-bb68-4888-9763-ee5e2b382528" />

Azure Synapse Analytics is a unified analytics service that combines:

ADF (Data Ingestion / Orchestration)

Spark (Big Data Processing / Lakehouse)

SQL Warehousing (Analytics & BI Reporting)

That is why you often see it summarized as:

ADF + Spark + Data Warehousing = Azure Synapse Analytics

2. Resources Created When You Create a Synapse Workspace
   
   i) Synapse Analytics Workspace
   This is the main analytics workspace where you:

Write Spark code,Run SQL queries,Build data pipelines,Connect Power BI

   ii)Default Synapse Storage Account

   This is an Azure Data Lake Gen2 account automatically created by Synapse.

Key points about default storage:

Synapse has full access to it by default

No permission issues

Used internally for:

Spark metadata

Temporary files

Logs

⚠ Industry Best Practice:

We do NOT store business data here

Instead, we use external ADLS accounts (like dataprojectsynapes) for:

Raw data

Curated data

Production workloads

3. How Synapse Accesses Data Lake (Very Important Interview Concept)
❓ Question:

How does Synapse access data stored in ADLS without usernames/passwords?

✅ Answer:

Using Managed Identity

4. Managed Identity (System Assigned Identity)

Every Synapse workspace has a System Assigned Managed Identity.

What this identity does:

Acts like a service account

Used to authenticate Synapse with:

ADLS

Azure SQL

Key Vault

Other Azure services

What we do practically:

Go to ADLS

Open IAM (Access Control)

Assign role:

Storage Blob Data Contributor

Assign it to:

Synapse Workspace Managed Identity

👉 After this, Synapse can read/write data securely.

Interview keyword:

“Azure Synapse uses System Assigned Managed Identity for secure access.”

Lakehouse Concept (Core Understanding)
Traditional Data Warehouse:

Data stored inside database

Expensive storage

Less flexible

Lakehouse (Modern Approach):
Layer	Where Data Lives
Actual Data	Azure Data Lake (CSV / Parquet / Delta)
Metadata	SQL / Spark Metastore
Query Engine	Serverless SQL / Spark
🔍 What Actually Happens?

Let’s say:

Data is stored in ADLS as CSV files

You create a table in Synapse using Serverless SQL

CREATE EXTERNAL TABLE sales_ext (
    order_id INT,
    amount FLOAT,
    order_date DATE
)
WITH (
    LOCATION = 'sales/',
    DATA_SOURCE = my_datalake,
    FILE_FORMAT = csv_format
);

Now when you run:
SELECT * FROM sales_ext;


👉 Synapse:

Does NOT move data into database

Reads metadata

Fetches data directly from ADLS

Returns results

This is called: Lakehouse Architecture

7. Why Lakehouse = Data Lake + Data Warehouse?
Feature	Data Lake	Data Warehouse	Lakehouse
Cheap Storage	✅	❌	✅
SQL Queries	❌	✅	✅
Scalability	✅	❌	✅
BI Friendly	❌	✅	✅

👉 Best of both worlds

Why We Store Data in Data Lake (Cost Reason)
Key Reason:

💰 Cost Optimization

ADLS storage = very cheap

Data volume = grows exponentially

Database storage = very expensive

Industry Reality:

We store TBs or PBs of data in ADLS

We store metadata + compute logic in Synapse

Azure Synapse Analytics is a unified analytics platform that brings together data ingestion, big data processing, and data warehousing capabilities in a single service. When a Synapse workspace is created, Azure automatically provisions two key resources: the Synapse workspace itself and a default Azure Data Lake Gen2 storage account. The Synapse workspace is where engineers and analysts work—writing Spark code, executing SQL queries, building pipelines, and connecting reporting tools like Power BI.

The default storage account is primarily used internally by Synapse for system data such as logs, temporary files, and Spark metadata. Although Synapse can easily read and write data to this default storage without permission issues, in real-world projects it is not recommended to store business or production data there. Instead, organizations use external ADLS Gen2 accounts as enterprise data lakes for better governance, security, and scalability.

From an access and security perspective, Azure Synapse connects to Azure Data Lake using Managed Identity, specifically a system-assigned managed identity. This identity acts like a secure service account automatically created with the Synapse workspace. Rather than using client IDs, secrets, or passwords, we simply grant this managed identity the required role (such as Storage Blob Data Contributor) on the target Data Lake. For example, if sales data is stored in an ADLS account called azuredeltalakestorage09, we assign the Synapse workspace identity access to that storage. Once this is done, Synapse can securely read and write data without any manual credential management. This approach is widely used in production environments and is a common interview topic.

<img width="1920" height="661" alt="image" src="https://github.com/user-attachments/assets/b6e81906-4aa4-4ad3-b502-7614b1e6e6bf" />

When working with Spark in Synapse, databases are often referred to as lake databases, which is conceptually the same as the lakehouse terminology used in Databricks. The idea is that data is not physically stored inside a traditional database engine. Instead, the actual data files—such as CSV, Parquet, or Delta files—reside in Azure Data Lake, while the database only stores metadata. Metadata includes information such as column names, data types, file locations, and schema definitions. For example, if customer data exists as CSV files in ADLS, a lake database can be created in Synapse Spark to define the schema. When a query is executed, Spark reads the metadata and then directly processes the data from the lake.

This leads to the core lakehouse concept, which combines the low-cost, scalable storage of a data lake with the query and analytics capabilities of a data warehouse. In a lakehouse architecture, data remains in the data lake, but an abstraction layer allows users to query it using SQL as if it were stored in a traditional database.

For instance, using Synapse Serverless SQL Pool, we can create an external table over Parquet files stored in ADLS. When a user runs a SELECT * query, Synapse does not copy the data into a database. Instead, it reads the metadata, fetches the data directly from the lake, applies the schema, and returns the results. To the user, it feels like querying a normal data warehouse, but behind the scenes the data remains in the data lake.

Organizations prefer this approach mainly because of cost and scalability. Storing large and rapidly growing datasets in databases is expensive, while Azure Data Lake provides very low-cost storage. By keeping data in ADLS and using Synapse for compute and metadata management, companies achieve significant cost savings while still supporting advanced analytics and BI workloads. For example, an e-commerce company might ingest raw order data using Azure Data Factory into ADLS, transform it using Spark in Synapse, expose curated datasets through Serverless SQL as external tables, and finally connect Power BI to Synapse for reporting. This end-to-end flow demonstrates how Azure Synapse Analytics enables modern data engineering using the lakehouse architecture.

Azure Synapse Serverless SQL Pool is a fully serverless analytics engine, which means there is no infrastructure to manage and no fixed compute to provision. It automatically scales up and down based on query workload and charges only for the amount of data processed by queries. A very important characteristic of Serverless SQL is that it does not store data physically inside the database. Instead, it directly queries data stored in Azure Data Lake using an abstraction layer, which perfectly supports the Lakehouse architecture. In practical terms, this allows organizations to keep all their data in low-cost Azure Data Lake storage while still querying it using standard SQL, just like a traditional data warehouse.

To access data stored in the Silver layer of the data lake, Synapse Serverless SQL provides a powerful function called OPENROWSET(). This function allows SQL queries to directly read files stored in Azure Data Lake formats such as Parquet, CSV, or JSON. For example, if curated calendar data is stored in Parquet format inside the Silver layer path /silver/AdventureWorks_Calendar/, we can query it directly using a SQL statement that points to the Data Lake URL. When this query runs, Synapse does not copy the data anywhere; instead, it reads the Parquet files on demand, applies schema inference or metadata, and returns the results. This is the abstraction layer that makes querying lake data feel like querying a database table.

For this direct access to work securely, Synapse requires proper permissions on the Data Lake. This is achieved by assigning the Storage Blob Data Contributor role to the Synapse workspace’s managed identity at the storage account or container level. Once this role is assigned, Synapse can seamlessly read data from the Data Lake without using secrets, keys, or credentials in code. This approach is both secure and production-ready and is the recommended pattern in enterprise Azure architectures.

Once data can be queried using OPENROWSET(), the next step is to prepare it for reporting and analytics, especially for tools like Power BI. While OPENROWSET() is powerful, it is not ideal for end users or BI tools to repeatedly reference file paths and formats. To solve this, we create SQL views on top of OPENROWSET() queries. A view stores only the SQL logic, not the data itself. Whenever the view is queried, it dynamically fetches the latest data from the Data Lake. For example, a view named gold.calendar can be created that selects data from the Silver layer calendar files. This view becomes a logical table that analysts and BI tools can query easily.

These views are typically organized in a Gold layer schema, which represents business-ready, curated datasets. The Gold layer does not contain raw files; instead, it contains views that apply business logic, transformations, and naming conventions. For instance, a gold.customer view may expose cleaned customer attributes such as name, gender, and birthdate, sourced from Silver layer Parquet files. This separation ensures that raw and curated data remain in the lake, while business users interact only with trusted, optimized views.

Once Gold layer views are created, querying becomes extremely simple. Users no longer need to know where the data is stored or how it is formatted. A query like SELECT * FROM gold.customer; behaves exactly like querying a traditional database table, even though the data physically resides in Azure Data Lake. This makes the solution highly accessible to managers, stakeholders, data analysts, and BI developers. Power BI can directly connect to Synapse Serverless SQL and consume these Gold views as datasets for dashboards and reports.

This approach demonstrates a complete Lakehouse reporting flow: data is stored cheaply in Azure Data Lake, accessed securely via managed identity, queried dynamically using Serverless SQL and OPENROWSET(), abstracted using Gold layer views, and finally consumed by Power BI for reporting. This architecture delivers scalability, cost efficiency, security, and ease of use—making it the preferred design for modern Azure data engineering solutions.

<img width="1920" height="436" alt="image" src="https://github.com/user-attachments/assets/75b7be17-5b06-4897-8d76-19f1026d1495" />
<img width="1601" height="476" alt="image" src="https://github.com/user-attachments/assets/caeccbd9-06a5-4513-a930-d86eadc6e82c" />
<img width="1920" height="439" alt="image" src="https://github.com/user-attachments/assets/7afe658f-926b-4405-ace2-915cc79b2428" />
<img width="1911" height="815" alt="image" src="https://github.com/user-attachments/assets/ad78c4d7-7f94-4f4f-9ea8-9026562526a0" />
<img width="1394" height="411" alt="image" src="https://github.com/user-attachments/assets/837a3bcf-ee74-4538-ab9a-3aed0c7508d2" />

In Azure Synapse Analytics, the difference between managed tables, external tables, and views is primarily about where the data is stored and who controls it. A managed table stores both the metadata and the actual data inside the analytics system itself. This model is common in systems like Databricks managed tables, where the platform controls the storage location. However, in Synapse Serverless SQL, we typically avoid managed tables because the core design follows the Lakehouse architecture, where data must remain in Azure Data Lake rather than being stored inside the database engine.

An external table, on the other hand, stores only metadata in Synapse, while the actual data files physically reside in Azure Data Lake Storage (ADLS). This gives full control over data location, retention, and reuse. For example, when sales data is written into the Gold layer of ADLS as Parquet files, an external table can be created in Synapse that points to that folder. Synapse then treats those files like a database table, but the data remains permanently stored in the lake. This is the preferred approach in enterprise-grade analytics solutions.

Before creating an external table in Synapse, three mandatory steps must be completed. First, a database master key must be created. This key is required to securely store credentials inside the database. Without this step, Synapse cannot manage authentication details securely. Second, a database scoped credential is created, usually using Managed Identity. This tells Synapse to authenticate to ADLS using the workspace’s system-assigned managed identity instead of secrets or access keys. Third, an external data source is defined, which points to a specific container or folder in Azure Data Lake, such as the Silver or Gold layer. Along with this, an external file format (for example, Parquet with Snappy compression) is created so Synapse understands how the data files are structured.

Once these prerequisites are complete, Synapse is ready to create external tables. In a typical Lakehouse pipeline, data is first queried using views over the Silver layer, and then materialized into the Gold layer using CETAS (Create External Table As Select). CETAS is a powerful command that executes a query and writes the query result as physical files in ADLS, while simultaneously registering metadata as an external table. For example, a gold.sales view may combine and clean Silver-layer sales data, and CETAS can be used to write this curated data into a folder like /gold/extsales/. This creates both the Parquet files in ADLS and an external table in Synapse that points to those files.

At this point, querying the data using a view or an external table may return the same results, which often raises a common question: why do we need external tables if views already work? The key difference is data persistence. A view stores only the SQL logic, not the data. Every time a view is queried, it re-reads data from the underlying source, such as Silver-layer files. In contrast, an external table stores the actual output data in the Data Lake, making it reusable, shareable, and independent of the original query logic. This allows the data to be retained, versioned, and consumed by multiple downstream systems without reprocessing.

By using CETAS and external tables, we gain physical data presence in the Gold layer, which is visible directly in the Azure Data Lake container. This confirms that the Gold layer is not just a logical abstraction but a real, governed data layer. These Gold external tables are ideal for Power BI consumption, as they provide stable schemas, optimized formats, and predictable performance. Power BI connects to Synapse Serverless SQL, queries the external tables, and delivers dashboards to business users.

Finally, this entire flow highlights an important responsibility of a data engineer. The role is not limited to ingesting and transforming data but extends to serving data effectively to stakeholders. Establishing secure and efficient connectivity between Synapse and Power BI, organizing Gold-layer datasets, and ensuring data usability for analysts and managers are all part of delivering a complete analytics solution. In modern data engineering, success is measured not just by pipelines built, but by how well data is delivered, governed, and consumed across the organization.

Once the Gold layer external tables are created in Azure Synapse, the final and most important step is serving the data to business users, and this is where Power BI comes into the picture. Power BI does not connect directly to Azure Data Lake; instead, it connects to SQL endpoints exposed by Azure Synapse Analytics. These SQL endpoints act as a secure and standardized interface between Synapse and downstream BI tools.

Azure Synapse provides multiple SQL endpoints, such as Dedicated SQL endpoint, Serverless SQL endpoint, and Development endpoint. In a Lakehouse-based architecture where data resides in Azure Data Lake and is exposed using views or external tables, we primarily use the Serverless SQL endpoint. This endpoint allows Power BI to query external tables and views without requiring any dedicated compute infrastructure. The endpoint URL is available directly in the Synapse workspace overview section and is explicitly labeled as the Serverless SQL endpoint.

To establish the connection, Power BI Desktop is opened and the SQL Server connector is selected. In the server name field, the Serverless SQL endpoint URL of the Synapse workspace is provided. Along with this, the database name (for example, Dataproject_database) is entered. Authentication is done using SQL authentication or Microsoft Entra ID, depending on how the Synapse workspace is configured. Once credentials are validated, Power BI establishes a live connection with Synapse Serverless SQL.

After the connection is successful, Power BI displays all available schemas, views, and external tables from the selected database. At this stage, the Gold layer external tables (such as gold.extsales or gold.customer) become visible. These tables are ideal for reporting because they represent curated, business-ready data and are already optimized in Parquet format within the Data Lake. The data engineer’s responsibility is to ensure that only clean, governed datasets are exposed at this layer.

Once the data is loaded into Power BI, analysts and business users can start building visualizations such as bar charts, line charts, KPIs, and trend analyses. For example, a report can show count of orders by year, total customers, or sales growth trends over time using fields like OrderDate, CustomerKey, and OrderNumber. Power BI automatically creates date hierarchies (Year, Quarter, Month, Day), enabling flexible time-based analysis without additional modeling effort.

The dashboards shown in the images demonstrate how Power BI consumes Synapse data seamlessly and transforms it into actionable insights. Metrics like Total Customers (56.05K) and year-wise customer growth trends are calculated directly from the Gold layer external tables. Importantly, Power BI users do not need to know anything about Azure Data Lake paths, CETAS, or file formats. All complexity is abstracted by Synapse, which reinforces the importance of proper Gold layer design.

From a data engineering perspective, this step highlights that the role does not end with building pipelines or transforming data. A data engineer is also responsible for enabling data consumption, ensuring secure connectivity, optimal performance, and correct data exposure to downstream systems like Power BI. By connecting Synapse Serverless SQL to Power BI, we complete the Lakehouse lifecycle—ingest, transform, serve, and visualize—and successfully deliver business value from raw data.

<img width="1341" height="739" alt="image" src="https://github.com/user-attachments/assets/b6357724-6f14-482b-9393-f230afc1c9f8" />

<img width="1333" height="619" alt="image" src="https://github.com/user-attachments/assets/e67efb94-70df-414e-9a38-ff8855a00733" />

<img width="1268" height="347" alt="image" src="https://github.com/user-attachments/assets/c6e6142a-d5cf-4750-a2e0-8552f1d12f39" />







   





