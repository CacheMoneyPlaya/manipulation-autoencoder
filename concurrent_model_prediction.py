import os
import argparse
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import load_model

# Constants
SEQUENCE_LENGTH = 200

def normalize_data(data):
    # Normalize each column as a sample of a larger population
    for column in data.columns:
        # Min-max scaling
        min_val = data[column].min()
        max_val = data[column].max()
        data[column] = (data[column] - min_val) / (max_val - min_val)

    return data

def process_file(file_path, model):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Select the last 200 rows, if available
    if len(df) >= SEQUENCE_LENGTH:
        df = df.iloc[-SEQUENCE_LENGTH:]

        # Normalize the data
        df = normalize_data(df)

        # Prepare the data for prediction
        X = np.array(df.values).reshape(1, SEQUENCE_LENGTH, len(df.columns))

        # Predict using the loaded model
        prediction = model.predict(X)[0][0] * 100  # Convert to percentage

        # Get the file name for output
        file_name = os.path.basename(file_path)
        symbol = file_name.split('_')[0]

        print(f"{symbol} @ {prediction:.2f}%")

def main():
    parser = argparse.ArgumentParser(description="Model inference on CSV data.")
    parser.add_argument("-f", "--model_file", help="Model file to load", required=True)
    args = parser.parse_args()

    # Load the LSTM model
    model = load_model(args.model_file)

    # Run the process every 5 minutes
    while True:
        try:
            for file_name in os.listdir('concurrent_data'):
                if file_name.endswith('.csv'):
                    file_path = os.path.join('concurrent_data', file_name)
                    process_file(file_path, model)

            # Wait for 5 minutes
            time.sleep(300)
        except KeyboardInterrupt:
            print("Process interrupted. Exiting gracefully.")
            break

if __name__ == "__main__":
    main()
