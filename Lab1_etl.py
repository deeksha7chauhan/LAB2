from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    '2_stock_etl',
    default_args=default_args,
    description='A simple DAG to fetch stock data for multiple symbols and process it using @task decorator with Snowflake',
    schedule_interval='*/10 * * * *',  # Runs every 10 minutes
    start_date=days_ago(1),
    catchup=False,
)

# Function to return a Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

# Task 1: Extracting stock data from Alpha Vantage using the @task decorator for Apple and Tesla stocks
@task
def extract(symbol):
    api_key = Variable.get('alpha_vantage_api_key')
    url_template = Variable.get("url")
    url = url_template.format(symbol=symbol, vantage_api_key=api_key)

    response = requests.get(url)
    data = response.json()

    return data

# Task 2: Get the last 90 days of stock prices exclung the extra records as market is closed on the weekend
@task
def return_last_90d_price(symbol):
    vantage_api_key = Variable.get('alpha_vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'

    r = requests.get(url)
    data = r.json()

    results = []  # List to hold the last 90 days of stock info
    ninety_days_ago = datetime.today() - timedelta(days=90)

    for d in data.get("Time Series (Daily)", {}):
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= ninety_days_ago:
            price_data = {
                "date": d,
                "open": data["Time Series (Daily)"][d]["1. open"],
                "high": data["Time Series (Daily)"][d]["2. high"],
                "low": data["Time Series (Daily)"][d]["3. low"],
                "close": data["Time Series (Daily)"][d]["4. close"],
                "volume": data["Time Series (Daily)"][d]["5. volume"],
                "symbol": symbol
            }
            results.append(price_data)

    return results

# Task 3: Process the data using the @task decorator
@task
def transform(stock_data: list):
    processed_data = []
    for entry in stock_data:
        processed_data.append(entry)

    print(f"Processed Data: {json.dumps(processed_data, indent=2)}")
    return processed_data

# Task 4: Load data into Snowflake table with transactions
@task
def load(records):
    if not records:
        print("No records to load.")
        return

    target_table = "STOCK.data.market_dat"
    
    # Get Snowflake cursor
    cur = return_snowflake_conn()
    
    try:
        # Begin transaction
        cur.execute("BEGIN;")
        print("Transaction started.")

        # Create the table if it does not exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            date DATE,
            symbol VARCHAR,
            open NUMBER,
            high NUMBER,
            low NUMBER,
            close NUMBER,
            volume NUMBER,
            PRIMARY KEY (date, symbol)
        )
        """)

        # Loading records into the target table
        for r in records:
            date = r['date']
            symbol = r['symbol']
            open_price = r['open']
            high_price = r['high']
            low_price = r['low']
            close_price = r['close']
            volume = r['volume']

            print(f"Inserting data for {date}, Symbol={symbol}: Open={open_price}, High={high_price}, Low={low_price}, Close={close_price}, Volume={volume}")

            # Using incremental load to load the data in the table
            sql = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT TO_DATE('{date}', 'YYYY-MM-DD') AS date, '{symbol}' AS symbol, 
                          {open_price} AS open, {high_price} AS high, 
                          {low_price} AS low, {close_price} AS close, 
                          {volume} AS volume) AS source
            ON target.date = source.date AND target.symbol = source.symbol
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (date, symbol, open, high, low, close, volume)
                VALUES (source.date, source.symbol, source.open, source.high, source.low, source.close, source.volume);
            """
            
            cur.execute(sql)
            print(f"Data inserted/updated for {date} and {symbol}.")

        # Commit transaction
        cur.execute("COMMIT;")
        print("Transaction committed successfully.")

    except Exception as e:
        # Rollback transaction in case of error
        cur.execute("ROLLBACK;")
        print(f"Transaction rolled back due to error: {str(e)}")

# Define the dag and task dependencies
with dag:
    symbols = ['AAPL', 'TSLA']
    for symbol in symbols:
        stock_data = extract(symbol)
        last_90_days_data = return_last_90d_price(symbol)
        transformed_data = transform(last_90_days_data)
        load(transformed_data)
