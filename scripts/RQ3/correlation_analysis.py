import sys
sys.stdout.reconfigure(encoding='utf-8')  # type: ignore # Ensure UTF-8 encoding for output
import pandas as pd
from scipy.stats import pearsonr, spearmanr

# Load the integrated dataset
df = pd.read_csv("data/new_rq3/final_dataset.csv")

metrics = [
    ("Flights_2023",    "Flights from Schiphol"),
    ("Passengers_2023","Passengers at destination")
]

print("Correlation analysis:")
for col, label in metrics:
    x = df[col]
    y = df["Avg_ATC_Delay_2023"]
    r,  p_r   = pearsonr(x, y)
    rho, p_s = spearmanr(x, y)
    print(f"• {label} vs Delay → "
          f"Pearson r = {r:.2f} (p = {p_r:.3f}), "
          f"Spearman ρ = {rho:.2f} (p = {p_s:.3f})")