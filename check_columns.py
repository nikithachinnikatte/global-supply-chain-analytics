import pandas as pd
import os

# === CHANGE THIS LINE TO YOUR REAL CSV PATH ===
CSV_PATH = r"C:\Users\POWERBI PROJECT\data\raw\DataCoSupplyChainDataset.csv"
# Example:
# CSV_PATH = r"C:\Users\POWERBI PROJECT\data\raw\DataCoSupplyChainDataset.csv"

print("Using path:", CSV_PATH)
print("File exists?", os.path.isfile(CSV_PATH))

if not os.path.isfile(CSV_PATH):
    raise FileNotFoundError("CSV file not found. Check the path and file name.")

# Try reading with a more flexible encoding to avoid UnicodeDecodeError
df = pd.read_csv(CSV_PATH, encoding="latin1", on_bad_lines="skip")

print("\nColumns in the CSV:")
print(df.columns)

print("\nFirst 5 rows:")
print(df.head())#check_columns
