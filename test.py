import csv
import requests
import os
import time
from multiprocessing import Pool

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
    return float(data[9]) - (float(data[5]) - float(data[9]))

# Function to save data to CSV
def save_to_csv(args):
    filename, open_interest, open_interest_value, kline_data = args

    headers = ["sum_open_interest", "sum_open_interest_value", "open", "high", "low", "close", "volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "volume_delta"]

    open_interest = int(round(float(open_interest)))
    taker_buy_volume = int(round(float(kline_data[9])))
    volume_delta = calculate_volume_delta(kline_data)

    data = [open_interest, open_interest_value, kline_data[1], kline_data[2], kline_data[3], kline_data[4], kline_data[5], kline_data[8], taker_buy_volume, kline_data[10], volume_delta]

    if not os.path.exists(filename):
        print(f"CSV file not found for {symbol}. Skipping.")
        return

    try:
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)

        print(f"Data appended to {filename}.")
    except Exception as e:
        print(f"Failed to write data to {filename}: {str(e)}")

# Run the script concurrently for each CSV file in the concurrent_data folder
def process_csv_file(csv_file):
    symbol = csv_file.split('_')[0]

    open_interest, open_interest_value = fetch_open_interest_data(symbol)
    kline_data = fetch_kline_data(symbol)

    if open_interest is not None and kline_data is not None:
        filename = os.path.join('concurrent_data', csv_file)
        save_to_csv((filename, open_interest, open_interest_value, kline_data))

if __name__ == "__main__":
    # Reset CSV files (clear content) only if it's the first run
    if not os.path.exists('is_first_run.txt'):
        for csv_file in os.listdir('concurrent_data'):
            if csv_file.endswith('.csv'):
                filename = os.path.join('concurrent_data', csv_file)
                with open(filename, mode='w', newline='') as file:
                    pass  # Clear content
        with open('is_first_run.txt', 'w') as file:
            pass  # Create a marker file

    csv_files = [csv_file for csv_file in os.listdir('concurrent_data') if csv_file.endswith('.csv')]

    with Pool(4) as pool:
        pool.map(process_csv_file, csv_files)

    while True:
        time.sleep(300)  # Wait for 5 minutes before the next run
        with Pool(4) as pool:
            pool.map(process_csv_file, csv_files)
