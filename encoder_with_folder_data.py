import argparse
import os
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
from sklearn.metrics.pairwise import cosine_similarity

# Run this with a specified folder contianing normalized data to test data with their MSE generated
# I.E If you have some training data you want to test or you create some new training data

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
    # Argument parsing
    parser = argparse.ArgumentParser(description="Calculate MSE for CSV data using the autoencoder model.")
    parser.add_argument("-d", "--folder", required=True, help="Folder containing CSV files")
    args = parser.parse_args()

    # Load the autoencoder model
    model = load_model('autoencoder_model.h5')

    # Get a list of all CSV files in the specified folder
    csv_files = [f for f in os.listdir(args.folder) if f.endswith('.csv')]

    for csv_file in csv_files:
        # Load the CSV data into a DataFrame
        csv_path = os.path.join(args.folder, csv_file)
        df = pd.read_csv(csv_path)

        # Assuming the input data is the first 200 rows and 12 columns
        input_data = df.values[:25, :]

        # Reshape the data to match the model's expected input shape for an autoencoder
        input_data = input_data.reshape((1, 25, 12))

        # Predict using the autoencoder model
        decoded_data = model.predict(input_data)

        # Get the symbol from the file name
        symbol = os.path.basename(csv_file).split('_')[0]

        cs = calculate_cosine_similarity(input_data, decoded_data)

        print(f"{symbol} - Cosine Sim: {cs:.4f}")
