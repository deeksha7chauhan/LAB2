from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import logging
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook


# DAG definition
default_args = {
    'owner': 'deeksha',
    'email': ['deeksha7chauhan@gmail.com'],
    'retries': 1,
}
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = '/opt/airflow/dbt'   

conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id='stock_price_analysis_lab2',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ETL', 'dbt', 'Analytics'],
    schedule_interval='@daily',
     default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }}
     ,   
) as dag:
    @task
    def fetch_data(symbols=['AAPL', 'TSLA']):
        try:
            vantage_api = Variable.get('alpha_vantage_api_key')
            logging.info("Using Alpha Vantage API key.")
            results = []
            for symbol in symbols:
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api}'
                response = requests.get(url)
                response.raise_for_status() 
                data = response.json()
                print(data)
                if "Time Series (Daily)" not in data:
                    logging.error(f"No data found for symbol: {symbol}")
                    continue
                for date, stock_info in data["Time Series (Daily)"].items():
                    stock_info["date"] = date
                    stock_info["symbol"] = symbol
                    results.append(stock_info)
                logging.info(f"Fetched data successfully for {symbol}")
            return results
        except Exception as e:
            logging.error(f"ERROR!! fetching data: {str(e)}")
            raise

    create_warehouse_db_schema = SnowflakeOperator(
        task_id='create_warehouse_db_schema',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Create warehouse, database, and schemas
        CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
        WITH WAREHOUSE_SIZE = 'SMALL'
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

        CREATE DATABASE IF NOT EXISTS STOCK_PRICE_DB;
        CREATE SCHEMA IF NOT EXISTS STOCK_PRICE_DB.RAW_DATA;
        """,
        warehouse='COMPUTE_WH'
    )

    @task
    def insert_data_into_snowflake(results):
        try:
            hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
            conn = hook.get_conn()
            with conn.cursor() as cur:
                # Use the RAW_DATA schema
                cur.execute("USE DATABASE STOCK_PRICE_DB;")
                cur.execute("USE SCHEMA RAW_DATA;")
                
                # Create the target table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS VANTAGE_API (
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume INT,
                    date DATE,
                    symbol VARCHAR(20),
                    PRIMARY KEY (date, symbol)
                );
                """
                cur.execute(create_table_sql)
                #temp table for staging
                cur.execute("""
                CREATE OR REPLACE TEMPORARY TABLE VANTAGE_API_STAGE LIKE VANTAGE_API;
                """)     
                #Insertion Data
                insert_values = []
                for record in results:
                    insert_values.append((
                        float(record['1. open']),
                        float(record['2. high']),
                        float(record['3. low']),
                        float(record['4. close']),
                        int(record['5. volume']),
                        record['date'],
                        record['symbol']
                    ))
                
                #Data into temp staging
                insert_sql = """
                INSERT INTO VANTAGE_API_STAGE (open, high, low, close, volume, date, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cur.executemany(insert_sql, insert_values)
                logging.info("Data inserted into staging table successfully.")
                
                #Merge
                merge_sql = """
                MERGE INTO VANTAGE_API AS target
                USING VANTAGE_API_STAGE AS source
                ON target.date = source.date AND target.symbol = source.symbol
                WHEN MATCHED THEN
                    UPDATE SET
                        open = source.open,
                        high = source.high,
                        low = source.low,
                        close = source.close,
                        volume = source.volume
                WHEN NOT MATCHED THEN
                    INSERT (open, high, low, close, volume, date, symbol)
                    VALUES (source.open, source.high, source.low, source.close, source.volume, source.date, source.symbol);
                """
                cur.execute(merge_sql)
                logging.info("Data merged into target table successfully.")
                
                #DROP staging
                cur.execute("DROP TABLE IF EXISTS VANTAGE_API_STAGE;")
                logging.info("Temporary staging table dropped.")
        except Exception as e:
            logging.error(f"Error inserting data into Snowflake: {str(e)}")
            raise
        finally:
            conn.close()
    conn = BaseHook.get_connection('snowflake_conn')
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )
    #Dependency
    fetched_data = fetch_data()
    create_warehouse_db_schema >> insert_data_into_snowflake(fetched_data) >> dbt_run >> dbt_test >> dbt_snapshot
