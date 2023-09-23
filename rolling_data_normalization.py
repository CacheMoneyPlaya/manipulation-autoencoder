import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def normalize_csv(input_csv, output_csv):
    # Read the CSV into a DataFrame
    df = pd.read_csv(input_csv)

    # Normalize each column as a sample of a larger population
    for column in df.columns:
        # Min-max scaling
        min_val = df[column].min()
        max_val = df[column].max()
        df[column] = (df[column] - min_val) / (max_val - min_val)

    # Save the normalized data to a new CSV file
    df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    input_csv = "input.csv"  # Replace with your CSV file path
    output_csv = "output.csv"  # Replace with the desired output CSV file path

    normalize_csv(input_csv, output_csv)
    print("Normalization completed. Data saved to", output_csv)
