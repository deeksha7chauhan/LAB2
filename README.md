# *Data Warehouse Lab 2: ETL/ELT Pipeline with Airflow and DBT*

This repository contains two Apache Airflow DAGs and a dbt project for implementing an ETL/ELT pipeline targeting stock data. The Airflow DAGs handle orchestration of data pipelines, while dbt is used for modular SQL transformations. Below is an overview of the flow for each DAG and the dbt project structure.

---

## *Installation:*

In order to run these files in Airflow and dbt, the following steps need to be followed:

1. Install **Apache Airflow** and set up a local or cloud-based instance.
2. Install **dbt** and configure the `profiles.yml` file with your database connection details.
3. Install the following Python libraries:
   - `apache-airflow`
   - `dbt-core`
4. Configure Airflow connection IDs and variables (if needed) for your environment.
5. Place the two Airflow DAG `.py` files in the `dags` folder of your Airflow setup.
6. Clone the `dbt_project` folder into your working directory.

---

## **1. ETL DAG (Lab1_etl.py)**

This Airflow DAG orchestrates an ETL pipeline that extracts, transforms, and loads stock data into a data warehouse.

### *Steps:*

1. *Extract Data:*
   - Fetches raw stock data from a source (e.g., API or CSV).
   
2. *Transform Data:*
   - Cleans and preprocesses the raw data to match the schema required by the database.

3. *Load Data:*
   - Inserts the processed data into a staging table in the data warehouse.

### *Task Flow:*

1. Extract stock data → 
2. Clean and preprocess the data → 
3. Load data into the data warehouse.

---

## **2. ELT DAG with dbt (build_elt_with_dbt.py)**

This Airflow DAG integrates with dbt to perform ELT operations, focusing on transformations directly in the database.

### *Steps:*

1. *Load Data:*
   - Inserts raw stock data into a database using Airflow tasks.
   
2. *Run dbt Transformations:*
   - Executes dbt commands (`dbt run`, `dbt test`) to transform raw data into models and data marts.

3. *Validate Data:*
   - Runs dbt tests to validate model integrity.

### *Task Flow:*

1. Load raw stock data → 
2. Execute dbt transformations → 
3. Validate model outputs.

---

## **3. dbt Project Overview**

### *Key Models:*

1. **Raw Models:**
   - `raw/stock_data.sql`: Represents the raw stock data ingested into the warehouse.

2. **Transformations:**
   - `transformations/stock_analysis.sql`: Analyzes stock data (e.g., calculating moving averages, RSI).

3. **Data Marts:**
   - `marts/stock_mart.sql`: Final datasets prepared for BI tools.

4. **Snapshots:**
   - `snapshots/stock_snapshot.sql`: Tracks changes over time using Slowly Changing Dimensions (SCD).

---

## *Snowflake or Warehouse Table Structure:*

- *Raw Table:* Holds the ingested raw stock data.
- *Transformed Table:* Contains clean and processed data ready for analysis.
- *Data Marts:* Consolidated tables for reporting and visualization.

---

## Contact

Created by:
•⁠  ⁠*Nikhil Swami* ([nikhil.swami@sjsu.edu](mailto:nikhil.swami@sjsu.edu))
•⁠  ⁠*Deeksha Chauhan* ([deeksha.chauhan@sjsu.edu](mailto:deeksha.chauhan@sjsu.edu))
