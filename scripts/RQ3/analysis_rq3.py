import pandas as pd
from pathlib import Path

# 1. Paths
data_dir        = Path.cwd() / "data"
avia_par_file   = data_dir / "Eurostat/avia_par/avia_par_nl_2023.csv"
avia_paoa_file  = data_dir / "Eurostat/avia_paoa/estat_avia_paoa_filtered.csv"
arcgis_file     = data_dir / "ArcGIS/ArcGIS_data.csv"
atc_file        = data_dir / "ATC/atc_pre_departure_delays_2023.csv"

# 2. Load
avia_par = pd.read_csv(avia_par_file)
avia_paoa = pd.read_csv(avia_paoa_file)
arcgis = pd.read_csv(arcgis_file)
atc = pd.read_csv(atc_file)

# 3. Flights by destination ICAO (2023)
fp = avia_par[avia_par['TIME_PERIOD']==2023].copy()
# 'airp_pr' like "NL_EHAM_AE_OMAA" → dest = 4th segment
fp['Dest_ICAO'] = fp['airp_pr'].str.split('_').str[3]
flights = (fp.groupby('Dest_ICAO', as_index=False)['OBS_VALUE']
             .sum().rename(columns={'OBS_VALUE':'Flights_2023'})) # type: ignore

# 4. Passengers by airport ICAO (2023)
paoa = avia_paoa[avia_paoa['TIME_PERIOD']==2023].copy()
# 'rep_airp' like "AT_LOWG" → ICAO = second segment
paoa['Airport_ICAO'] = paoa['rep_airp'].str.split('_').str[1]
paoa = (paoa.groupby('Airport_ICAO', as_index=False)['OBS_VALUE']
          .sum().rename(columns={'OBS_VALUE':'Passengers_2023'})) # type: ignore

# 5. ATC delays weighted avg per ICAO (2023)
delay = atc[atc['YEAR']==2023].dropna(subset=['FLT_DEP_3','DLY_ATC_PRE_3']).copy()
delay['TotalDelayMin'] = delay['FLT_DEP_3'] * delay['DLY_ATC_PRE_3']
delay = (delay.groupby('APT_ICAO', as_index=False)
               .agg({'TotalDelayMin':'sum','FLT_DEP_3':'sum'}))
delay['Avg_ATC_Delay_2023'] = delay['TotalDelayMin'] / delay['FLT_DEP_3']
delay = delay[['APT_ICAO','Avg_ATC_Delay_2023']]

# 6. ArcGIS mapping (ICAO, IATA, geo, runway, country)
arc = arcgis.rename(columns={
    'GPS-Code':'ICAO_Code',
    'IATA-Code':'IATA_Code',
    'Runway length (ft)':'LongestRunwayLength',
    'Runway Surface':'LongestRunwaySurface',
    'ISO-Country':'ISO_Country'
})[
    ['ICAO_Code','IATA_Code','Name','Latitude','Longitude',
     'LongestRunwayLength','LongestRunwaySurface','Type','ISO_Country']
].drop_duplicates()

# 7. Merge flights ↔ passengers on ICAO
df = flights.merge(paoa, left_on='Dest_ICAO', right_on='Airport_ICAO', how='inner')

# 8. Merge in ATC delays
df = df.merge(delay, left_on='Dest_ICAO', right_on='APT_ICAO', how='inner')

# 9. Enrich via ArcGIS
df = df.merge(arc, left_on='Dest_ICAO', right_on='ICAO_Code', how='left')

# 10. Hub vs Regional
df['HubStatus'] = df.apply(
    lambda r: 'Hub' if (
        r['Passengers_2023'] > 1_000_000 or
        (pd.notna(r['LongestRunwayLength']) and r['LongestRunwayLength'] > 12000)
    ) else 'Regional', axis=1
)

# 11. Save
out = data_dir / "new_rq3"
out.mkdir(parents=True, exist_ok=True)
df.to_csv(out / "final_dataset.csv", index=False)

print(f"Final dataset: {len(df)} airports")
