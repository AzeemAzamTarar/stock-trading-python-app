import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- Loading API Key ----------------

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# ---------------- Config ----------------------

LIMIT = 1000

# ---------------- Initial Request -------------

print("Sending initial request to Polygon API...")

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
data = response.json()

tickers = []

# ---------------- Process First Page ----------

print("Processing first page of results...")

for ticker in data['results']:
    tickers.append(ticker)

print(f"Keys in response: {list(data.keys())}")
print(f"Tickers fetched so far: {len(tickers)}")

# ---------------- Pagination Loop -------------

while data.get('next_url'):

    print(f"\nRequesting next page: {data['next_url']}")
    
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()

    print(f"Processing next page of results...")
    for ticker in data['results']:
        tickers.append(ticker)

    print(f"Total tickers fetched so far: {len(tickers)}")

    # ---- Pausing to avoid hitting rate limits ----

    print("⏳ Waiting 30 seconds before next request...")
    time.sleep(30)

# ---------------- Final Count -----------------

print("\n✅ Finished fetching all pages.")
print(f"Total tickers fetched: {len(tickers)}")

# ---------------- CSV Writing -----------------

example_ticker =  {
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

fieldnames = list(example_ticker.keys())

output_csv = 'tickers.csv'

print(f"\n Writing data to {output_csv}...")

with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)

print(f'✅ Wrote {len(tickers)} rows to {output_csv}')
