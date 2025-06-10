import pandas as pd
import glob
import os

# 1. Point this to the folder with your per-country CSVs
input_folder = "data/Eurostat/avia_par"
output_folder = "data/Eurostat/processed"
os.makedirs(output_folder, exist_ok=True)

# 2. Build a list of all the CSV paths
csv_paths = glob.glob(f"{input_folder}/avia_par_*.csv")

# 3. Read and concatenate them into one DataFrame
df = pd.concat((pd.read_csv(p, dtype=str) for p in csv_paths),
               ignore_index=True)

# 4. (Optional) inspect the columns
print("Columns found:", df.columns.tolist())

# 5. Filter to only Jan & Feb 2025
df = df[df["TIME_PERIOD"].isin(["2024-01", "2024-02", "2024-03"])]

# 6. Convert the flight-count column to numeric
#    (adjust name if yours differs: e.g. 'flightCount', 'flights', etc.)
df["FLIGHTCOUNT"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce").fillna(0)

# 7. Aggregate total flights per main airport & month
# 1. Group and sum into a DataFrame
agg = (
    df
    .groupby(["airp_pr", "TIME_PERIOD"], as_index=False)
    .agg({"OBS_VALUE": "sum"})          # returns a DataFrame
    .rename(columns={
        "airp_pr":    "iata_code",
        "TIME_PERIOD":     "month",
        "OBS_VALUE":     "monthly_flights"
    })
)

out_csv = f"{output_folder}/avia_par_all_2025M01-02.csv"
agg.to_csv("data/Eurostat/processed/avia_par_all_2025M01-02.csv", index=False)
print("Saved aggregated flights to", out_csv)