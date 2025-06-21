import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Load
df = pd.read_csv(Path.cwd() / "data/new_rq3/final_dataset.csv")

# 1. Histogram: Avg Delay
plt.figure()
plt.hist(df['Avg_ATC_Delay_2023'], bins=20)
plt.xlabel("Avg ATC Delay 2023 (min)")
plt.ylabel("Count of Airports")
plt.title("Distribution of Avg ATC Delays")
plt.tight_layout()
plt.savefig("data/new_rq3/plots/delay_hist.png")
plt.close()

# 2. Top 10 by delay
top10 = df.nlargest(10, 'Avg_ATC_Delay_2023')
plt.figure()
plt.bar(top10['IATA_Code'], top10['Avg_ATC_Delay_2023'])
plt.xlabel("Airport (IATA)")
plt.ylabel("Avg Delay (min)")
plt.title("Top 10 Airports by Delay")
plt.tight_layout()
plt.savefig("data/new_rq3/plots/top10_delay.png")
plt.close()

# 3. Scatter: Passengers vs Delay
plt.figure()
plt.scatter(df['Passengers_2023'], df['Avg_ATC_Delay_2023'])
plt.xlabel("Passengers 2023")
plt.ylabel("Avg ATC Delay (min)")
plt.title("Passengers vs Delay")
plt.tight_layout()
plt.savefig("data/new_rq3/plots/passengers_vs_delay.png")
plt.close()

# 4. Scatter: Flights vs Delay
plt.figure()
plt.scatter(df['Flights_2023'], df['Avg_ATC_Delay_2023'])
plt.xlabel("Flights 2023")
plt.ylabel("Avg ATC Delay (min)")
plt.title("Flights vs Delay")
plt.tight_layout()
plt.savefig("data/new_rq3/plots/flights_vs_delay.png")
plt.close()
