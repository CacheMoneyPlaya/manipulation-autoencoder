import csv
import requests
import os

# Function to fetch open interest data
def fetch_open_interest_data(symbol):
    endpoint = f"https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=1"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        if data and data[0].get('sumOpenInterest') and data[0].get('sumOpenInterestValue'):
            return data[0]['sumOpenInterest'], data[0]['sumOpenInterestValue']
    else:
        print(f"Failed to fetch open interest data for {symbol}.")

    return None, None

# Function to fetch kline data
def fetch_kline_data(symbol):
    endpoint = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit=1"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        return data[0]
    else:
        print(f"Failed to fetch kline data for {symbol}.")
        return None

# Function to save data to CSV
def save_to_csv(filename, open_interest, open_interest_value, kline_data):
    headers = ["sum_open_interest", "sum_open_interest_value", "open", "high", "low", "close", "volume", "count", "taker_buy_volume", "taker_buy_quote_volume"]

    # Round sum_open_interest and taker_buy_volume to integers
    open_interest = int(round(float(open_interest)))
    taker_buy_volume = int(round(float(kline_data[9])))

    data = [open_interest, open_interest_value, kline_data[1], kline_data[2], kline_data[3], kline_data[4], kline_data[5], kline_data[8], taker_buy_volume, kline_data[10]]

    # Check if the file exists
    if not os.path.exists(filename):
        print(f"CSV file not found for {symbol}. Skipping.")
        return

    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # Write headers
            writer.writerow(data)  # Write data

        print(f"Data saved to {filename}.")
    except Exception as e:
        print(f"Failed to write data to {filename}: {str(e)}")

# Run the script for each CSV file in the concurrent_data folder
for csv_file in os.listdir('concurrent_data'):
    if csv_file.endswith('.csv'):
        symbol = csv_file.split('_')[0]  # Extract symbol from the filename
        open_interest, open_interest_value = fetch_open_interest_data(symbol)
        kline_data = fetch_kline_data(symbol)
        if open_interest is not None and kline_data is not None:
            filename = os.path.join('concurrent_data', csv_file)
            save_to_csv(filename, open_interest, open_interest_value, kline_data)
