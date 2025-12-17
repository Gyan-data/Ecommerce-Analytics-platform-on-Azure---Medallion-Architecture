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

###AZURE SYNAPES ANALYTICS


