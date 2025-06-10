#!/usr/bin/env python3
"""
analysis.py

Enhanced pipeline for RQ3:
“Which destinations out of AMS exhibit significant departure‐delay patterns?”
"""

import os, json, glob
from datetime import datetime
import pandas as pd
import numpy as np
import scipy.stats as stats
from dateutil import parser
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import branca.colormap as bcm
from matplotlib.colors import Normalize, to_hex

# -----------------------------------------------------------------------------
# 1) Load & preprocess KLM flight data (Q1-2025)
# -----------------------------------------------------------------------------
def load_klm(path="data/KLM/klm_flights_Q1_2025.json") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    data = json.load(open(path))
    flights = data.get("operationalFlights", [])
    rows = []
    for fl in flights:
        date = fl.get("flightScheduleDate")
        dest = (fl.get("route") or [None])[-1]
        for leg in fl.get("flightLegs", []):
            dep = leg.get("departureInformation", {})
            if dep.get("airport",{}).get("code")!="AMS": continue
            times  = dep.get("times",{})
            sched  = times.get("scheduled")
            actual = times.get("actual")
            if not sched or not actual: continue
            sdt = parser.isoparse(sched); adt=parser.isoparse(actual)
            delay = (adt-sdt).total_seconds()/60
            rows.append({"flightDate":date,"destinationIATA":dest,"delayMinutes":delay})
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("No valid AMS departures found")
    df["month"] = pd.to_datetime(df["flightDate"]).dt.to_period("M").astype(str)
    print(f"[KLM] Loaded {len(df)} flights, months: {df['month'].unique()}")
    os.makedirs("data/KLM/processed", exist_ok=True)
    df.to_csv("data/KLM/processed/flights_Q1_2025.csv", index=False)
    return df

# -----------------------------------------------------------------------------
# 2) Derive volume proxy: flight counts + summary
# -----------------------------------------------------------------------------
def compute_flight_counts(df: pd.DataFrame) -> pd.DataFrame:
    summary = (df.groupby("destinationIATA")
                 .agg(num_flights=("delayMinutes","count"),
                      avg_delay   =("delayMinutes","mean"),
                      med_delay   =("delayMinutes","median"))
                 .reset_index())
    print(f"[Flights] Summary for {len(summary)} destinations")
    return summary

# -----------------------------------------------------------------------------
# 3) Load & classify airports (hub vs regional, intra-EU vs intercontinental)
# -----------------------------------------------------------------------------
def load_airports(path="data/ArcGIS/ArcGIS_data.csv") -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df.rename(columns={
        "IATA-Code":"destinationIATA",
        "ISO-Country":"iso_country",
        "Runway length (ft)":"runway_ft"
    }).dropna(subset=["destinationIATA","iso_country"])
    df["runway_m"]   = df["runway_ft"]*0.3048
    df["isHub"]      = df["runway_m"]>=2400
    EU = {'AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU',
          'IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK'}
    df["isIntraEU"]  = df["iso_country"].isin(EU)
    print(f"[ArcGIS] {len(df)} airports → hubs:{df.isHub.sum()}, intra-EU:{df.isIntraEU.sum()}")
    return df[["destinationIATA","isHub","isIntraEU"]]

# -----------------------------------------------------------------------------
# 4) Merge all into analysis table
# -----------------------------------------------------------------------------
def build_analysis_table() -> pd.DataFrame:
    df_f = load_klm()
    df_s = compute_flight_counts(df_f)
    df_a = load_airports()
    df  = df_s.merge(df_a, on="destinationIATA", how="inner")
    print(f"[Merge] {len(df)} destinations combined")
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/analysis_table.csv", index=False)
    return df

# -----------------------------------------------------------------------------
# 5) Statistical tests (H1–H3)
# -----------------------------------------------------------------------------
def run_tests(df: pd.DataFrame):
    if len(df)<2:
        print("Not enough data for tests."); return
    # H1: flight count vs avg delay
    r,p  = stats.pearsonr(df["num_flights"], df["avg_delay"])
    rho,p2 = stats.spearmanr(df["num_flights"], df["med_delay"])
    print(f"H1: Pearson r={r:.2f}(p={p:.3f}), Spearman ρ={rho:.2f}(p={p2:.3f})")

    # H2: hub vs regional
    hubs = df[df.isHub]["avg_delay"]
    regs = df[~df.isHub]["avg_delay"]
    if len(hubs)>1 and len(regs)>1:
        t,p3 = stats.ttest_ind(hubs, regs, nan_policy="omit")
        print(f"H2: t={t:.2f}(p={p3:.3f})")
    else:
        print("H2: insufficient groups")

    # H3: intra-EU vs intercontinental
    eu  = df[df.isIntraEU]["avg_delay"]
    non = df[~df.isIntraEU]["avg_delay"]
    if len(eu)>0 and len(non)>0:
        u,p4 = stats.mannwhitneyu(eu, non)
        print(f"H3: U={u:.1f}(p={p4:.3f})")
    else:
        print("H3: insufficient groups")

# -----------------------------------------------------------------------------
# 6) Advanced visualizations + interactive map
# -----------------------------------------------------------------------------
def make_plots(df: pd.DataFrame):
    os.makedirs("outputs", exist_ok=True)

    # 6a) LOWESS bubble scatter (summary df)
    plt.figure(figsize=(8,6))
    sizes = (df["num_flights"] / df["num_flights"].max()) * 400
    sns.regplot(x="num_flights", y="avg_delay", data=df,
                scatter=False, lowess=True, color="red")
    plt.scatter(df["num_flights"], df["avg_delay"], s=sizes, alpha=0.6)
    plt.xscale("log")
    plt.xlabel("Flight Count (log scale)")
    plt.ylabel("Avg Delay (min)")
    plt.title("Delay vs Flight Count (LOWESS)")
    plt.savefig("outputs/delay_vs_count.png")
    plt.show()

    # 6b) Violin plots (use raw for distributions)
    raw = load_klm()  # reload raw flight-level data
    merged = pd.merge(raw, df, on="destinationIATA")
    for col, title in [("isHub", "Hub vs Regional"), ("isIntraEU", "Intra-EU vs Intercontinental")]:
        plt.figure(figsize=(6,4))
        sns.violinplot(x=col, y="delayMinutes", data=merged, inner="quartile")
        plt.title(title)
        plt.ylabel("Delay (min)")
        plt.savefig(f"outputs/{col}_violin.png")
        plt.show()

    # 6c) Interactive delay map (summary df used for setting radius/color)
    arc = pd.read_csv("data/ArcGIS/ArcGIS_data.csv", low_memory=False)
    arc = arc.rename(columns={"IATA-Code":"destinationIATA","Latitude":"lat","Longitude":"lon"})
    arc = arc[["destinationIATA","lat","lon"]].dropna()
    df_map = pd.merge(df, arc, on="destinationIATA", how="inner")
    vmin, vmax = df_map["avg_delay"].min(), df_map["avg_delay"].max()
    norm = Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.cm.get_cmap("YlOrRd")

    m = folium.Map(location=[52.3083,4.7681], zoom_start=4, tiles="CartoDB.Positron")
    for _,r in df_map.iterrows():
        color = to_hex(cmap(norm(r["avg_delay"])))
        folium.CircleMarker(
            [r["lat"],r["lon"]],
            radius=5 + np.log1p(r["num_flights"]),
            color=color, fill=True, fill_opacity=0.7,
            popup=f"{r['destinationIATA']}<br>Flights:{r['num_flights']}<br>Delay:{r['avg_delay']:.1f}"
        ).add_to(m)
    folium.LayerControl().add_to(m)
    m.save("outputs/delay_map.html")
    print("Map → outputs/delay_map.html")

    # 6d) Heatmap by Weekday (raw data)
    raw = load_klm()  # reload raw flight-level data
    raw["dayOfWeek"] = pd.to_datetime(raw["flightDate"]).dt.day_name()

    # pivot: index=airport, columns=dayOfWeek, values=mean delay
    pivot = (
        raw
        .pivot_table(index="destinationIATA",
                     columns="dayOfWeek",
                     values="delayMinutes",
                     aggfunc="mean",
                     fill_value=np.nan)
        .reindex(
            ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
            axis=1,
            fill_value=np.nan
        )
    )


    plt.figure(figsize=(10,8))
    sns.heatmap(pivot, cmap="crest", linewidths=0.5, linecolor="gray",
                cbar_kws={"label": "Avg Delay (min)"})
    plt.title("Average Delay by Destination & Weekday")
    plt.xlabel("Day of Week")
    plt.ylabel("Destination IATA")
    plt.tight_layout()
    plt.savefig("outputs/heatmap_weekday.png")
    plt.show()

    # 6e) Tail‐Risk Plot (raw + summary)
    raw   = load_klm()  # flight‐level
    # Compute 95th‐percentile delay per destination
    p95 = (
        raw.groupby("destinationIATA")["delayMinutes"]
           .quantile(0.95)
           .reset_index()
           .rename(columns={"delayMinutes":"p95_delay"})
    )

    # Compute summary (num_flights, etc.)
    summary = compute_flight_counts(raw)

    # Merge them
    tail = pd.merge(summary, p95, on="destinationIATA", how="inner")

    # Now tail has destinationIATA, num_flights, avg_delay, med_delay, and p95_delay
    plt.figure(figsize=(8,6))
    sns.scatterplot(
        data=tail,
        x="num_flights",
        y="p95_delay",
        hue="p95_delay",
        palette="flare",
        size="num_flights",
        sizes=(20,200),
        legend="brief"
    )
    plt.xscale("log")
    plt.xlabel("Flight Count (log)")
    plt.ylabel("95th %-ile Delay (min)")
    plt.title("Tail Risk of Delays vs Volume")
    plt.savefig("outputs/tail_risk.png")
    plt.show()
# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__=="__main__":
    df = build_analysis_table()
    run_tests(df)
    make_plots(df)
    print("➡️ Analysis complete.")