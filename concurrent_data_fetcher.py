import os
import requests
from multiprocessing import Pool
import time
from datetime import datetime, timedelta
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import load_model
import numpy as np
import csv

# Load the pre-trained autoencoder model
model = load_model('autoencoder_model.h5')

# Function to fetch open interest data
def fetch_open_interest_data(symbol, limit=1):
    endpoint = f"https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit={limit}"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        open_interest_data = [(item.get('sumOpenInterest'), item.get('sumOpenInterestValue')) for item in data if item.get('sumOpenInterest') and item.get('sumOpenInterestValue')]
        return open_interest_data
    else:
        print(f"Failed to fetch open interest data for {symbol}.")

    return None

# Function to fetch kline data
def fetch_kline_data(symbol, limit=1):
    endpoint = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit={limit}"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        kline_data = [item for item in data if len(item) > 1]
        return kline_data
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
    filename, open_interest_data, kline_data = args

    headers = ["sum_open_interest", "sum_open_interest_value", "open", "high", "low", "close", "volume", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "volume_delta"]

    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # Write headers

            for oi, kline in zip(open_interest_data, kline_data):
                open_interest = int(round(float(oi[0])))
                open_interest_value = oi[1]
                taker_buy_volume = int(round(float(kline[9])))
                volume_delta = calculate_volume_delta(kline)

                data = [open_interest, open_interest_value, kline[1], kline[2], kline[3], kline[4], kline[5], kline[7], kline[8], taker_buy_volume, kline[10], volume_delta]
                writer.writerow(data)

        print(f"Data appended to {filename}.")
    except Exception as e:
        print(f"Failed to write data to {filename}: {str(e)}")

# Dictionary to track whether it's the first run for each file
first_run_dict = {}

# Function to process CSV file
def process_csv_file(csv_file):
    global first_run_dict

    symbol = csv_file.split('_')[0]

    # Check if it's the first run for this file
    if symbol not in first_run_dict:
        open_interest_data = fetch_open_interest_data(symbol, limit=200)
        kline_data = fetch_kline_data(symbol, limit=200)

        # Set the first run flag to False after the first run
        first_run_dict[symbol] = False
    else:
        open_interest_data = fetch_open_interest_data(symbol, limit=1)
        kline_data = fetch_kline_data(symbol, limit=1)

    if open_interest_data is not None and kline_data is not None:
        filename = os.path.join('concurrent_data', csv_file)
        save_to_csv((filename, open_interest_data, kline_data))

# Function to normalize and min-max a column of data
def normalize_column(column_data):
    # Reshape the data for MinMaxScaler
    column_data = column_data.values.reshape(-1, 1)

    # Apply Min-Max scaling
    scaler = MinMaxScaler()
    normalized_column = scaler.fit_transform(column_data)

    return normalized_column

if __name__ == "__main__":
    # Run the script periodically at 5-minute intervals within an hour
    while True:
        now = datetime.now()
        if now.minute % 1 == 0:  # Run at 5-minute intervals
            csv_files = [csv_file for csv_file in os.listdir('concurrent_data') if csv_file.endswith('.csv')]

            with Pool(6) as pool:
                pool.map(process_csv_file, csv_files)

            # Process and normalize each CSV file
            for csv_file in csv_files:
                csv_file_path = os.path.join('concurrent_data', csv_file)
                try:
                    # Load the CSV file into a DataFrame
                    df = pd.read_csv(csv_file_path)

                    # Iterate through each column
                    for col in df.columns:
                        # Normalize and min-max the column
                        df[col] = normalize_column(df[col])

                    # Save the normalized data back to the same CSV file
                    df.to_csv(csv_file_path, index=False)
                    print(f"Normalized and saved {csv_file_path}")

                    # Read the data (excluding the header) and convert to numpy array
                    data = np.genfromtxt(csv_file_path, delimiter=',', skip_header=1)

                    # Reshape the data to match the model's expected input shape for an autoencoder
                    data = data.reshape((1, 200, 12))

                    # Predict using the autoencoder model
                    decoded_data = model.predict(data)

                    # Get the symbol from the file name
                    symbol = csv_file.split('_')[0]

                    # Calculate MSE as a percentage
                    mse_percentage = np.mean(np.square(data - decoded_data))

                    # Invert the percentage (higher similarity should result in higher percentage)
                    inverted_percentage = 100 - mse_percentage

                    print(f"{symbol} - {mse_percentage:.4f}")

                except Exception as e:
                    print(f"Error processing {csv_file_path}: {str(e)}")

            time.sleep(300)  # Delay for 5 minutes before running again
        else:
            time.sleep(1)  # Wait for 1 second before checking again
