import pandas as pd

# Path to your Parquet file
file_path = "data/yellow/yellow_tripdata_2019-01.parquet"

# Read the Parquet file into a DataFrame
try:
    df = pd.read_parquet(file_path)
    print("Parquet file loaded successfully!")
except Exception as e:
    print(f"Error loading Parquet file: {e}")
    exit()

# Display the first few rows of the DataFrame
print("\nFirst 5 rows of the DataFrame:")
print(df.head())

# Display column names and data types
print("\nColumn names and data types:")
print(df.dtypes)

# Display the shape of the DataFrame (rows, columns)
print(f"\nShape of the DataFrame: {df.shape}")

# Display summary statistics
print("\nSummary statistics:")
print(df.describe(include="all"))

# Optional: Display null values count per column
print("\nNull values per column:")
print(df.isnull().sum())

# Optional: Display sample data
print("\nRandom sample of data:")
print(df.sample(5))
