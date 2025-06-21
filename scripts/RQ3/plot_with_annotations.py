import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Loading data
df       = pd.read_csv("data/new_rq3/final_dataset.csv")
hotspots = pd.read_csv("data/new_rq3/above_avg_airports.txt", 
                       sep=":", names=["IATA","Info"], header=None)
hotspots["IATA"] = hotspots["IATA"].str.strip()

out_dir = Path("data/new_rq3/plots")
out_dir.mkdir(parents=True, exist_ok=True)

def annotate_scatter(x_col, y_col, fname, xlabel, ylabel, title):
    plt.figure(figsize=(6,5))
    colors = df['HubStatus'].map({'Hub':'red','Regional':'blue'})
    plt.scatter(df[x_col], df[y_col], c=colors, alpha=0.6)
    # Annotate hotspots
    subset = df[df['IATA_Code'].isin(hotspots['IATA'])]
    for _, r in subset.iterrows():
        plt.text(r[x_col], r[y_col], r['IATA_Code'], # type: ignore
                 fontsize=9, fontweight='bold')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_dir / fname)
    plt.close()

# Passengers vs. Delay
annotate_scatter(
    x_col="Passengers_2023",
    y_col="Avg_ATC_Delay_2023",
    fname="passengers_vs_delay_annotated.png",
    xlabel="Passengers 2023",
    ylabel="Avg ATC Delay (min)",
    title="Passengers vs ATC Delay (above-avg labelled)"
)

# Flights vs. Delay
annotate_scatter(
    x_col="Flights_2023",
    y_col="Avg_ATC_Delay_2023",
    fname="flights_vs_delay_annotated.png",
    xlabel="Flights 2023",
    ylabel="Avg ATC Delay (min)",
    title="Flights vs ATC Delay (above-avg labelled)"
)