import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import argparse

def analyze_data(input_csv):
    # Read the CSV into a DataFrame
    df = pd.read_csv(input_csv)

    # Select columns of interest
    columns_of_interest = ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
                           "open", "high", "low", "close", "volume", "quote_volume", "count",
                           "taker_buy_volume", "taker_buy_quote_volume", "volume_delta"]

    # Ensure the required columns are present in the CSV
    for col in columns_of_interest:
        if col not in df.columns:
            print(f"Column '{col}' not found in the CSV. Please provide a valid CSV.")
            return

    # Sort the DataFrame by create_time in ascending order
    df.sort_values(by='create_time', ascending=True, inplace=True)

    # Initialize variables to track used indices and count for output CSV names
    used_indices = set()
    count = 0

    # Get the base filename (without extension) from the input CSV path
    base_filename = os.path.splitext(os.path.basename(input_csv))[0]
    subfolder_name = f"{base_filename.split('_')[0]}_training_data"  # First element when split by '_' with "_training_data" appended

    # Create a subfolder based on the base filename (first element)
    subfolder_path = os.path.join(os.path.dirname(input_csv), subfolder_name)
    os.makedirs(subfolder_path, exist_ok=True)

    # Iterate through the DataFrame with a rolling window of 6 rows
    for i in range(len(df) - 5):
        window = df.iloc[i:i+6]
        first_close = window['close'].iloc[0]
        sixth_close = window['close'].iloc[-1]

        # Check for an increase of more than 5% between the 1st and 6th close values
        if sixth_close > first_close * 1.025:
            # Check if there are at least 200 rows before the 1st row of the rolling window
            if i >= 200:
                # Check if the 200 rows have not been used in previous rolling windows
                if all(idx not in used_indices for idx in range(i-199, i+1)):
                    used_indices.update(range(i-199, i+1))

                    # Extract the previous 200 rows of data
                    training_data = df.iloc[i-200:i]  # Original 200 rows

                    # Remove 'create_time' and 'symbol' columns
                    training_data = training_data.drop(columns=['create_time', 'symbol'])

                    # Apply Min-Max scaling to normalize between 0 and 1
                    scaler = MinMaxScaler()
                    training_data[columns_of_interest[2:]] = scaler.fit_transform(training_data[columns_of_interest[2:]])

                    # Create the output CSV filename
                    output_csv = os.path.join(subfolder_path, f"{base_filename}_{count}_training_data.csv")

                    # Save the training data to the CSV file
                    training_data.to_csv(output_csv, index=False, header=True)

                    count += 1

                    print(f"Training data saved to {output_csv}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze data and create training CSVs.")
    parser.add_argument("-f", "--file", help="Input CSV file name", required=True)
    args = parser.parse_args()

    # Analyze the data and save training CSVs
    analyze_data(args.file)
