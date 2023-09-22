import time
import os
import numpy as np
from tensorflow.keras.models import load_model
import pandas as pd

# Constants
FOLDER_PATH = "concurrent_data"  # Folder containing CSV files
MODEL_PATH = "manipulation_surge_model.h5"  # Trained model path
SEQUENCE_LENGTH = 200  # Number of time steps to consider for prediction

def load_latest_data():
    # Load the latest data from CSV files
    data = []
    for file_name in os.listdir(FOLDER_PATH):
        if file_name.endswith('.csv'):
            file_path = os.path.join(FOLDER_PATH, file_name)
            df = pd.read_csv(file_path, header=0)
            data.append(df.values[1:SEQUENCE_LENGTH+1])  # Exclude headers and take the first 200 rows

    return np.array(data)

def predict_manipulation(model, data):
    # Preprocess the data
    X = data[:, :-1]  # Features (all columns except the last)
    X = X.reshape(1, SEQUENCE_LENGTH, X.shape[1])  # Reshape to 3D shape (samples, time steps, features)

    # Predict manipulation chances
    prediction = model.predict(X)[0, 0]

    return prediction

if __name__ == "__main__":
    # Load the trained model
    model = load_model(MODEL_PATH)

    try:
        while True:
            # Load the latest data from CSV files
            data = load_latest_data()

            if data.size == 0:
                print("No new data available. Waiting for the next update...")
            else:
                # Extract the symbol from the file name
                symbol = os.listdir(FOLDER_PATH)[0].split('_')[0]

                # Predict manipulation chances
                prediction = predict_manipulation(model, data)

                # Output prediction
                print(f"{symbol} @ {prediction*100:.2f}%")

            # Wait for 5 minutes and 10 seconds
            time.sleep(5 * 60 + 10)

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting gracefully.")
