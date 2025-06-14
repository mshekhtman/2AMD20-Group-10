import pandas as pd

# Load the CSV file
df = pd.read_csv('Airports28062017_189278238873247918.csv')

# Filter out rows where 'type' is 'closed'
df = df[df['type'] != 'closed']

# Filter out rows with empty or missing 'iata_code'
df = df[df['iata_code'].notna() & (df['iata_code'] != '')]

# Save the cleaned data back to a CSV
df.to_csv('data/test_data/filtered_airports.csv', index=False)
