import os
import csv
import time
import requests
from dotenv import load_dotenv

# ---------------- Loading Environment Variables ----------------

# Loads API keys and other secrets from the .env file
load_dotenv()

# Retrieve Polygon API key from environment
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# ---------------- Configurations ----------------

LIMIT = 1000   # Number of tickers per request (max allowed by Polygon API)


def run_stock_job():

    # ---------------- Initial Request ----------------
    print("Sending initial request to Polygon API...")

    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    data = response.json()

    tickers = []  # List to store ticker data

    # ---------------- Processing First Page ----------------

    print("Processing first page of results...")

    for ticker in data['results']:
        tickers.append(ticker)

    print(f"Keys in response: {list(data.keys())}")
    print(f"Tickers fetched so far: {len(tickers)}")

    # ---------------- Pagination Loop ----------------

    # Keep fetching pages until 'next_url' is no longer available
    while data.get('next_url'):

        print(f"\nRequesting next page: {data['next_url']}")
        
        # Append API key again (Polygon requires it in every call)
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()

        print(f"Processing next page of results...")
        for ticker in data['results']:
            tickers.append(ticker)

        print(f"Total tickers fetched so far: {len(tickers)}")

        # ---- Pause to avoid hitting Polygon API rate limits ----
        print("⏳ Waiting 30 seconds before next request...")
        time.sleep(30)

    # ---------------- Final Count ----------------

    print("\n✅ Finished fetching all pages.")
    print(f"Total tickers fetched: {len(tickers)}")

    # ---------------- CSV Writing ----------------

    # Define CSV headers using a sample ticker
    example_ticker = {
        'ticker': 'ZWS',
        'name': 'Zurn Elkay Water Solutions Corporation',
        'market': 'stocks',
        'locale': 'us',
        'primary_exchange': 'XNYS',
        'type': 'CS',
        'active': True,
        'currency_name': 'usd',
        'cik': '0001439288',
        'composite_figi': 'BBG000H8R0N8',
        'share_class_figi': 'BBG001T36GB5',
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z'
    }

    fieldnames = list(example_ticker.keys())  # CSV column headers
    output_csv = 'tickers.csv'

    print(f"\n Writing data to {output_csv}...")

    # Write all ticker data to CSV
    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            # Ensure missing fields don't break CSV writing
            row = {key: t.get(key, '') for key in fieldnames}
            writer.writerow(row)

    print(f'✅ Wrote {len(tickers)} rows to {output_csv}')


# ---------------- Running Script ----------------

if __name__ == '__main__':
    run_stock_job()