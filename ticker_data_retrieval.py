import requests
import json
import zipfile
import io
import csv
import pandas as pd
from tqdm import tqdm
import os

# Function to download and merge data
def download_and_merge_data(url, payload):
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        data = response.json()
        download_items = data.get('data', {}).get('downloadItemList', [])
        merged_data = []

        for item in tqdm(download_items, desc='Downloading data'):
            download_url = item.get('url')
            day = item.get('day')
            filename = f'{symbol}-{day}.zip'  # Modified filename to include symbol

            zip_response = requests.get(download_url)

            if zip_response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zip_file:
                    for file_info in zip_file.infolist():
                        with zip_file.open(file_info) as csv_file:
                            # Read CSV data and append to merged_data
                            csv_reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
                            for i, row in enumerate(csv_reader):
                                if i > 0:  # Skip the header row
                                    merged_data.append(row)

            else:
                print(f"Failed to download zip file for {day}")

        return merged_data

    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Get user input for ticker, start date, and end date
symbol = input("Enter the ticker symbol (e.g., BTCUSDT): ")
start_date = input("Enter the start date (YYYY-MM-DD): ")
end_date = input("Enter the end date (YYYY-MM-DD): ")

# Define the endpoints using user input
url = 'https://www.binance.com/bapi/bigdata/v1/public/bigdata/finance/exchange/listDownloadData2'
payload_metrics = {
    "bizType": "FUTURES_UM",
    "productName": "metrics",
    "symbolRequestItems": [{
        "endDay": end_date,
        "granularityList": [],
        "interval": "daily",
        "startDay": start_date,
        "symbol": symbol
    }]
}
payload_klines = {
    "bizType": "FUTURES_UM",
    "productName": "klines",
    "symbolRequestItems": [{
        "endDay": end_date,
        "granularityList": ["5m"],
        "interval": "daily",
        "startDay": start_date,
        "symbol": symbol
    }]
}

# Download and merge metrics data
print("Downloading and merging metrics data...")
merged_metrics_data = download_and_merge_data(url, payload_metrics)

# Download and merge klines data
print("Downloading and merging klines data...")
merged_klines_data = download_and_merge_data(url, payload_klines)

# Merge data based on timestamps (assuming timestamps are the first column)
print("Merging data...")
merged_data = []
for metrics_row, klines_row in tqdm(zip(merged_metrics_data, merged_klines_data), desc='Merging data'):
    merged_row = metrics_row + klines_row[1:]  # Exclude the duplicate timestamp
    merged_data.append(merged_row)

# Create a DataFrame from merged_data
columns = ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio",
           "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio", "open", "high",
           "low", "close", "volume", "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "dummy"]

df = pd.DataFrame(merged_data, columns=columns)

# Drop specified columns
columns_to_drop = ["count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
                   "count_long_short_ratio", "sum_taker_long_short_vol_ratio", "dummy", "close_time"]

df.drop(columns=columns_to_drop, inplace=True)

# Calculate volume delta for each row
df['taker_buy_volume'] = df['taker_buy_volume'].astype(float)
df['volume'] = df['volume'].astype(float)
df['volume_delta'] = df['taker_buy_volume'] - (df['volume'] - df['taker_buy_volume'])

# Create a directory for the symbol if it doesn't exist
output_directory = symbol
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Write the DataFrame to a CSV file within the symbol-specific directory
output_filepath = os.path.join(output_directory, f'{symbol}_{start_date}_{end_date}.csv')
df.to_csv(output_filepath, index=False)

print(f"Data saved to {output_filepath}.")
