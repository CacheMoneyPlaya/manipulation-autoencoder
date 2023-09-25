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
from sklearn.metrics.pairwise import cosine_similarity

# Run this to grab latest 200 data points of data, format it, normalize it and then feed into the model
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'  # 0 (default) shows all, 2 suppresses INFO messages, 3 also suppresses WARNING messages

# Load the pre-trained autoencoder model
model = load_model('autoencoder_model.h5')
# Dictionary to track MSE for each symbol
mse_dict = {}


# Function to fetch open interest data
def fetch_open_interest_data(symbol, limit=1):
    endpoint = f"https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit={limit}"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        open_interest_data = [(item.get('sumOpenInterest'), item.get('sumOpenInterestValue'), item.get('timestamp')) for item in data if item.get('sumOpenInterest') and item.get('sumOpenInterestValue')]
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
    headers = ["timestamp", "sum_open_interest", "sum_open_interest_value", "open", "high", "low", "close", "volume", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "volume_delta"]

    try:
        data_list = []
        for oi, kline in zip(open_interest_data, kline_data):
            open_interest = int(round(float(oi[0])))
            open_interest_value = oi[1]
            taker_buy_volume = int(round(float(kline[9])))
            volume_delta = calculate_volume_delta(kline)
            data_list.append([oi[2], open_interest, open_interest_value, kline[1], kline[2], kline[3], kline[4], kline[5], kline[7], kline[8], taker_buy_volume, kline[10], volume_delta])

        df = pd.DataFrame(data_list, columns=headers)
        df.sort_values(by=['timestamp'], ascending=True, inplace=True)
        df.drop(columns=['timestamp'], inplace=True)
        df.to_csv(filename, index=False)  # Removed header=False to keep headers
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
        open_interest_data = fetch_open_interest_data(symbol, limit=25)
        kline_data = fetch_kline_data(symbol, limit=25)

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

def send_to_discord_webhook(content):
    webhook_url = 'https://discord.com/api/webhooks/1155503541562114109/84mkMKX4KLQ30EQi0SQSjWfx1HyuCRHZb0kwVOVwNRrSDGRbJIlSEkTT2s6ptIIiU3VB'
    payload = {'content': content}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(webhook_url, json=payload, headers=headers)

    if response.status_code == 204:
        print('Message sent to Discord webhook successfully.')
    else:
        print(f'Failed to send message to Discord webhook. Status code: {response.status_code}')

def calculate_cosine_similarity(data, decoded_data):
    """
    Calculate cosine similarity between the input data and decoded data.

    Parameters:
    data (ndarray): Input data with shape (1, 200, 12)
    decoded_data (ndarray): Decoded output data with shape (1, 12)

    Returns:
    float: Cosine similarity
    """
    # Flatten the input data and reshape the decoded data to match
    data_flat = data.reshape(-1, data.shape[-1])  # reshape to (200, 12)
    decoded_data_broadcasted = np.tile(decoded_data, (data.shape[1], 1))  # repeat decoded_data along axis 1 to match (200, 12)

    # Calculate Cosine Similarity
    cosine_sim = cosine_similarity(data_flat, decoded_data_broadcasted)

    return cosine_sim.mean()  # Return the mean cosine similarity


if __name__ == "__main__":
    # Run the script periodically at 5-minute intervals within an hour
    while True:
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        print("________________________________________________________________________")
        print("Current Timestamp with Hours and Minutes:", current_timestamp)
        csv_files = [csv_file for csv_file in os.listdir('concurrent_data') if csv_file.endswith('.csv')]

        with Pool(6) as pool:
            pool.map(process_csv_file, csv_files)

        # Get the current timestamp with hours and minutes
        # Print the timestamp
        print("Latest data fully retieved..")

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

                # Read the data (excluding the header) and convert to numpy array
                data = np.genfromtxt(csv_file_path, delimiter=',', skip_header=1)

                # Reshape the data to match the model's expected input shape for an autoencoder
                data = data.reshape((1, 25, 12))

                # Predict using the autoencoder model
                decoded_data = model.predict(data, verbose=0)

                # Get the symbol from the file name
                symbol = csv_file.split('_')[0]

                cosine_sim = calculate_cosine_similarity(data, decoded_data)

                message = symbol + ' with Cosine Sim of: ' + str(cosine_sim)

                print(message)

                # Send the message to Discord webhook if Cosine Sim is greater than 0.98
                if cosine_sim >= 0.95:
                    send_to_discord_webhook(message)

            except Exception as e:
                print(f"Error processing {csv_file_path}: {str(e)}")

        print('Round Complete')
        time.sleep(300)  # Delay for 5 minutes before running again
