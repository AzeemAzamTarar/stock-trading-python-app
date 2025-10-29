import os
import time
import requests
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

# ---------------- Load Environment Variables ----------------
# Loads API keys and Snowflake credentials from the .env file
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000  # Number of tickers per API request (max allowed by Polygon API)


# ---------------- Main Function ----------------
def run_stock_job():
    """
    Fetch all active stock tickers from the Polygon API
    and load them directly into Snowflake.
    """
    DS = datetime.now().strftime('%Y-%m-%d')
    tickers = []

    # ---------------- Initial Request ----------------
    print("Sending initial request to Polygon API...")

    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    data = response.json()

    # ---------------- Process First Page ----------------
    print("Processing first page of results...")
    for ticker in data["results"]:
        ticker["ds"] = DS
        tickers.append(ticker)

    print(f"Tickers fetched so far: {len(tickers)}")

    # ---------------- Pagination Loop ----------------
    while data.get("next_url"):
        print(f"\nRequesting next page: {data['next_url']}")
        response = requests.get(data["next_url"] + f"&apiKey={POLYGON_API_KEY}")
        data = response.json()

        for ticker in data["results"]:
            ticker["ds"] = DS
            tickers.append(ticker)

        print(f"Total tickers fetched so far: {len(tickers)}")
        print("⏳ Waiting 30 seconds before next request...")
        time.sleep(30)

    print("\n✅ Finished fetching all pages.")
    print(f"Total tickers fetched: {len(tickers)}")

    # ---------------- Schema Definition ----------------
    example_ticker = {
        "ticker": "ZWS",
        "name": "Zurn Elkay Water Solutions Corporation",
        "market": "stocks",
        "locale": "us",
        "primary_exchange": "XNYS",
        "type": "CS",
        "active": True,
        "currency_name": "usd",
        "cik": "0001439288",
        "composite_figi": "BBG000H8R0N8",
        "share_class_figi": "BBG001T36GB5",
        "last_updated_utc": "2025-09-11T06:11:10.586204443Z",
        "ds": "2025-09-25",
    }

    fieldnames = list(example_ticker.keys())

    # ---------------- Load to Snowflake ----------------
    load_to_snowflake(tickers, fieldnames)
    print(f"✅ Loaded {len(tickers)} rows to Snowflake successfully.")


# ---------------- Snowflake Loader Function ----------------
def load_to_snowflake(rows, fieldnames):
    """
    Creates the target table (if not exists) and inserts rows into Snowflake.
    Uses quoted identifiers to preserve case and ensure consistency.
    """

    connect_kwargs = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        **{k: v for k, v in connect_kwargs.items() if v},
        session_parameters={"CLIENT_TELEMETRY_ENABLED": False},
    )

    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv("SNOWFLAKE_TABLE", "stock_tickers")

            # Column type mapping
            type_overrides = {
                "ticker": "VARCHAR",
                "name": "VARCHAR",
                "market": "VARCHAR",
                "locale": "VARCHAR",
                "primary_exchange": "VARCHAR",
                "type": "VARCHAR",
                "active": "BOOLEAN",
                "currency_name": "VARCHAR",
                "cik": "VARCHAR",
                "composite_figi": "VARCHAR",
                "share_class_figi": "VARCHAR",
                "last_updated_utc": "TIMESTAMP_NTZ",
                "ds": "VARCHAR",
            }

            # ✅ Create table with quoted columns (preserve lowercase)
            columns_sql = ", ".join(
                [f'"{col}" {type_overrides.get(col, "VARCHAR")}' for col in fieldnames]
            )
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql})'
            cs.execute(create_table_sql)
            print(f"Ensured table exists: {table_name}")

            # ✅ Use quoted column names in INSERT
            column_list = ", ".join([f'"{c}"' for c in fieldnames])
            placeholders = ", ".join([f"%({c.lower()})s" for c in fieldnames])
            insert_sql = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'

            # ✅ Transform keys to lowercase to match placeholders
            transformed = [{k.lower(): t.get(k, None) for k in fieldnames} for t in rows]

            if transformed:
                print(f"Inserting {len(transformed)} rows into {table_name}...")
                cs.executemany(insert_sql, transformed)
                print("✅ Insert complete.")
            else:
                print("⚠️ No rows to insert.")

        except Exception as e:
            print(f"❌ Error during Snowflake load: {e}")
            raise

        finally:
            cs.close()

    finally:
        conn.close()
        print("Connection closed.")


# ---------------- Script Entry Point ----------------
if __name__ == "__main__":
    run_stock_job()
