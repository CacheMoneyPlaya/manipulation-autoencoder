import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.callbacks import ModelCheckpoint



# This needs to be run tomorrow to generate the model

# Constants
SEQUENCE_LENGTH = 200  # Number of time steps to consider for prediction

def load_training_data():
    data = []

    for folder_name in os.listdir():
        folder_path = os.path.join(os.getcwd(), folder_name)
        if os.path.isdir(folder_path):
            subfolder_name = folder_name + "_training_data"
            subfolder_path = os.path.join(folder_path, subfolder_name)
            if os.path.isdir(subfolder_path):
                for file_name in os.listdir(subfolder_path):
                    if file_name.endswith('.csv'):
                        file_path = os.path.join(subfolder_path, file_name)
                        df = pd.read_csv(file_path, header=0)
                        data.append(df.values[1:])  # Exclude headers

    if data:
        return np.vstack(data)
    else:
        return np.array([])  # Return an empty array if no training data found

def prepare_data(data):
    # Features and labels
    X = []
    y = []

    for i in range(len(data) - SEQUENCE_LENGTH):
        X.append(data[i:i+SEQUENCE_LENGTH, :-1])  # Features (all columns except the last)
        y.append(data[i+SEQUENCE_LENGTH, -1])     # Label (last column of the next sequence)

    X = np.array(X)
    y = np.array(y)

    return X, y

def create_lstm_model(input_shape):
    model = Sequential([
        LSTM(units=64, input_shape=input_shape, return_sequences=True),
        LSTM(units=32),
        Dense(units=1, activation='sigmoid')
    ])

    model.compile(loss='mean_squared_error', optimizer='adam')
    return model

def train_model(X_train, y_train, epochs=10, batch_size=32):
    model = create_lstm_model(X_train.shape[1:])
    checkpoint = ModelCheckpoint('manipulation_surge_model.h5', monitor='loss', save_best_only=True)
    model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, callbacks=[checkpoint])

if __name__ == "__main__":
    # Load training data
    training_data = load_training_data()

    if training_data.size == 0:
        print("No training data found.")
        exit(1)

    # Prepare the data
    X, y = prepare_data(training_data)

    # Train the model
    train_model(X, y)

    print("Training completed. Model saved as 'manipulation_surge_model.h5'.")
