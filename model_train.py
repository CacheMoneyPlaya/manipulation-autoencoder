import os
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

# Constants
SEQUENCE_LENGTH = 200
NUM_FEATURES = 12
BATCH_SIZE = 32
EPOCHS = 10

# Function to load and preprocess a single CSV file
def load_and_preprocess_data(file_path):
    df = pd.read_csv(file_path, header=0)

    # Remove empty rows and reset the index
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Exclude headers and any extra rows that exceed the desired sequence length
    data = df.iloc[:SEQUENCE_LENGTH].values

    # Reshape for a single sample
    data = data.reshape(1, SEQUENCE_LENGTH, NUM_FEATURES)
    return data

# Get the project root directory
project_root = os.path.dirname(os.path.abspath(__file__))

# Define the model
model = Sequential([
    LSTM(units=64, input_shape=(SEQUENCE_LENGTH, NUM_FEATURES), return_sequences=True),
    LSTM(units=32),
    Dense(units=NUM_FEATURES, activation='sigmoid')
])

model.compile(loss='mean_squared_error', optimizer='adam')
model.summary()

# Train the model on each CSV file
for root, dirs, files in os.walk(project_root):
    if 'training_data' in root:
        for file_name in files:
            if file_name.endswith('.csv'):
                file_path = os.path.join(root, file_name)
                print(f"Loading and preprocessing data from file: {file_name}")
                data = load_and_preprocess_data(file_path)
                # Train the model on the current file
                model.fit(data, data, epochs=EPOCHS, batch_size=BATCH_SIZE, shuffle=True)

# Save the trained model
model.save('autoencoder_model.h5')
print("Model saved as 'autoencoder_model.h5'.")
