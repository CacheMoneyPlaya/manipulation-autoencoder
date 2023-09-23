import os
import csv
import requests
from binance.client import Client

# Function to check if a symbol contains certain words
def contains_invalid_words(symbol):
    invalid_words = ['BULL', 'BEAR', 'UP', 'DOWN']
    for word in invalid_words:
        if word in symbol:
            return True
    return False

# Function to check if the response is empty
def is_empty_response(response):
    return len(response.json()) == 0

# Creates the concurrent tracking files
def get_usdt_symbols():
    client = Client()
    info = client.futures_exchange_info()
    symbols = [symbol['symbol'] for symbol in info['symbols'] if symbol['quoteAsset'] == 'USDT' and not contains_invalid_words(symbol['symbol'])]
    return symbols

def create_csv_file(symbol):
    endpoint = f"https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=1"
    response = requests.get(endpoint)

    if is_empty_response(response):
        print(f"No data available for {symbol}. Skipping file creation.")
        return

    file_name = f"{symbol}_concurrent_data.csv"
    file_path = os.path.join("concurrent_data", file_name)

    with open(file_path, "w", newline="") as csvfile:
        # Do not write any headers
        pass

if __name__ == "__main__":
    # Ensure the 'concurrent_data' directory exists
    os.makedirs("concurrent_data", exist_ok=True)

    # Get all USDT-traded symbols on Binance
    usdt_symbols = get_usdt_symbols()

    # Create CSV files for each symbol
    for symbol in usdt_symbols:
        create_csv_file(symbol)
        print(f"CSV file created for {symbol}: {symbol}_concurrent_data.csv")
