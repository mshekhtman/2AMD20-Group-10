import pandas as pd

# 1) Load your merged analysis table
df = pd.read_csv("data/processed/analysis_table.csv")
import pandas as pd

# Load region‐level delays and the full airport table
df_reg   = pd.read_csv("outputs/avg_delay_by_region.csv")
df_all   = pd.read_csv("data/processed/analysis_table.csv")
arc      = pd.read_csv("data/ArcGIS/ArcGIS_data.csv", low_memory=False)
arc      = arc.rename(columns={"IATA-Code":"destinationIATA","ISO-Country":"iso_country"})
df_full  = pd.merge(df_all, arc[["destinationIATA","iso_country"]], on="destinationIATA")

# Take the top 3 worst‐delayed regions
top_regions = df_reg.nlargest(3, "mean_delay")["region"].tolist()

for region in top_regions:
    print(f"\n--- Top Destinations in {region} (mean delay={df_reg.set_index('region').at[region,'mean_delay']:.1f} min) ---")
    sub = df_full[df_full.iso_country == region]
    # Sort by avg_delay desc, then by volume desc
    hot = sub.sort_values(["avg_delay","num_flights"], ascending=[False,False])
    print(hot[["destinationIATA","num_flights","avg_delay","isHub"]].head(5).to_string(index=False))


# 2) Pull in ISO‐Country from ArcGIS so we can filter to US
arc = pd.read_csv("data/ArcGIS/ArcGIS_data.csv", low_memory=False)
arc = arc.rename(columns={"IATA-Code":"destinationIATA","ISO-Country":"iso_country"})
arc = arc[["destinationIATA","iso_country"]].dropna()

# 3) Join to get country codes
df_all = pd.merge(df, arc, on="destinationIATA", how="inner")

# 4) Filter to US airports and sort by number of flights
us = df_all[df_all.iso_country == "US"]\
        .sort_values("num_flights", ascending=False)

# 5) Display the top US airports by flight count (and their average delay)
print("Top US Destinations by Flight Count (Q1-2025):\n")
print(us[["destinationIATA","num_flights","avg_delay","isHub","isIntraEU"]].head(10).to_string(index=False))
