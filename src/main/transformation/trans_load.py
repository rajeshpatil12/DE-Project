import pandas as pd

# Load the CSV into a pandas DataFrame
df = pd.read_csv(blob_name)

# Example transformation: Convert all column names to uppercase
df.columns = [col.upper() for col in df.columns]

# Another example: Filter rows where a certain column has a specific value
df_filtered = df[df['SOME_COLUMN'] == 'some_value']

# Save the transformed DataFrame to a new CSV
transformed_file = "transformed_data.csv"
df_filtered.to_csv(transformed_file, index=False)
print("Data transformation completed.")
