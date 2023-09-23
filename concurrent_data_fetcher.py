import csv
import requests
import os
from multiprocessing import Pool
import time

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

# Function to calculate volume_delta
def calculate_volume_delta(data):
    try:
        taker_buy_volume = float(data[9])
        volume = float(data[5])
        return taker_buy_volume - (volume - taker_buy_volume)
    except ValueError:
        print("Invalid data format. Could not calculate volume_delta.")
        return None

# Function to save data to CSV
def save_to_csv(args):
    filename, open_interest, open_interest_value, kline_data = args

    headers = ["sum_open_interest", "sum_open_interest_value", "open", "high", "low", "close", "volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "volume_delta"]

    open_interest = int(round(float(open_interest)))
    taker_buy_volume = int(round(float(kline_data[9])))
    volume_delta = calculate_volume_delta(kline_data)

    data = [open_interest, open_interest_value, kline_data[1], kline_data[2], kline_data[3], kline_data[4], kline_data[5], kline_data[8], taker_buy_volume, kline_data[10], volume_delta]

    try:
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            if os.path.getsize(filename) == 0:  # Check if file is empty
                writer.writerow(headers)  # Write headers if file is empty
            writer.writerow(data)
        print(f"Data appended to {filename}.")
    except Exception as e:
        print(f"Failed to write data to {filename}: {str(e)}")

# Reset CSV files (clear content)
def reset_csv_files():
    for csv_file in os.listdir('concurrent_data'):
        if csv_file.endswith('.csv'):
            filename = os.path.join('concurrent_data', csv_file)
            with open(filename, mode='w', newline='') as file:
                pass  # Clear content

# Function to process CSV file
def process_csv_file(csv_file):
    symbol = csv_file.split('_')[0]

    open_interest, open_interest_value = fetch_open_interest_data(symbol)
    kline_data = fetch_kline_data(symbol)

    if open_interest is not None and kline_data is not None:
        filename = os.path.join('concurrent_data', csv_file)
        save_to_csv((filename, open_interest, open_interest_value, kline_data))

# Run the script periodically with a delay of 5 minutes
if __name__ == "__main__":
    while True:
        csv_files = [csv_file for csv_file in os.listdir('concurrent_data') if csv_file.endswith('.csv')]

        reset_csv_files()

        with Pool(4) as pool:
            pool.map(process_csv_file, csv_files)

        time.sleep(300)  # Delay for 5 minutes before running again
