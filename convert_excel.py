
import pandas as pd
import os

# Input: Original Excel dataset
input_file = os.path.join(
    "data", "raw", "Online Retail.xlsx"
)

# Output: CSV dataset
output_file = os.path.join(
    "data", "processed", "online_retail.csv"
)

print("Reading Excel dataset...")

# Read Excel file
df = pd.read_excel(input_file, engine="openpyxl")

print("Excel dataset loaded successfully!")
print("Rows:", len(df))
print("Columns:", len(df.columns))

# Convert Excel into CSV
df.to_csv(output_file, index=False)

print("\nCSV conversion completed!")
print("Saved at:", output_file)
print("File exists:", os.path.exists(output_file))
