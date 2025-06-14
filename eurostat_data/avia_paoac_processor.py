import pandas as pd

# Load the TSV file with tab delimiter
df = pd.read_csv("eurostat_data/raw/estat_avia_paoac.tsv", sep='\t', dtype=str)

# The first column has compound keys like "A,FLIGHT,CAF_PAS,AT_LOWG,EXT_EU27_2020"
first_col_name = df.columns[0]
split_cols = ['freq', 'unit', 'tra_meas', 'rep_airp', 'partner']

# Split the compound key into separate columns
df[split_cols] = df[first_col_name].str.split(',', expand=True)

# Drop the original compound key column
df.drop(columns=[first_col_name], inplace=True)

# Drop rows early: keep only relevant 'unit' and 'tra_meas'
df = df[(df['unit'] != 'FLIGHT') & (df['tra_meas'] == 'PAS_BRD')]

# Strip whitespace from column names (safety)
df.columns = df.columns.str.strip()

# Metadata columns
metadata_cols = split_cols

# Identify date columns (exclude metadata)
date_cols = [col for col in df.columns if col not in metadata_cols]

# Keep only annual columns (4-digit year)
annual_cols = [col for col in date_cols if len(col) == 4 and col.isdigit()]

# Replace missing values like ":" or ": " with NaN
df = df.replace(r'^:\s*$', pd.NA, regex=True)

# Convert annual date columns to numeric values
for col in annual_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Create a DataFrame with metadata + annual data only
df = df[metadata_cols + annual_cols]

# Sum values per row (if needed)
# (Optional if you want a total column across years, else skip)
#df['total_all_years'] = df[annual_cols].sum(axis=1, skipna=True)

# Group by airport and sum all annual columns
airport_totals = df.groupby('rep_airp')[annual_cols].sum().reset_index()

# Extract ICAO code from 'rep_airp' (assuming format like 'AT_LOWG')
airport_totals['icao'] = airport_totals['rep_airp'].str.split('_').str[1]

# Reorder columns: rep_airp, icao, then annual years
cols = ['rep_airp', 'icao'] + annual_cols
airport_totals = airport_totals[cols]

# Save airport-level totals CSV
airport_totals.to_csv("eurostat_data/processed/estat_avia_paoac_airport_totals.csv", index=False)
print("Cleaned airport totals saved to 'eurostat_data/processed/estat_avia_paoac_airport_totals.csv'")
