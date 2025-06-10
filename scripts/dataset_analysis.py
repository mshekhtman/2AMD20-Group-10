# --- KLM dataset analysis ---

import json
import pandas as pd

with open('data/KLM/klm_flights_Q1_2025.json') as f:
    data = json.load(f)

# If the main key is 'operationalFlights'
flights = data['operationalFlights']
df_klm = pd.json_normalize(flights)
print(df_klm.head())
print("Columns KLM dataset: ",df_klm.columns.to_list())
summary = df_klm.describe()
print(summary)

# --- Eurostat datasets analysis ---

df_eurostat_passengers = pd.read_csv('data/Eurostat/avia_paoa/avia_paoa.csv')
df_eurostat_par = pd.read_csv('data/Eurostat/avia_par/processed/avia_par_all_2024-Q1.csv')

print(df_eurostat_passengers.head())
print("Columns paoa: ", df_eurostat_passengers.columns.to_list())
print(df_eurostat_par.head())
print("Columns par: ", df_eurostat_par.columns.to_list())

# Summary statistics for Eurostat data
summary_passengers = df_eurostat_passengers.describe()
summary_par = df_eurostat_par.describe()
print("--------- avia_paoa summary: ", summary_passengers)
print("----------avia_par summary: ", summary_par)


# --- ArcGIS dataset analysis ---
df_arcgis = pd.read_csv('data/ArcGIS/ArcGIS_data.csv')
print(df_arcgis.head())
print("Columns ArcGIS dataset: ", df_arcgis.columns.to_list())
# Summary statistics for ArcGIS data
summary_arcgis = df_arcgis.describe()
print("ArcGIS dataset summary: ", summary_arcgis)