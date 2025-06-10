#!/usr/bin/env python3
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

os.makedirs("outputs/plots", exist_ok=True)

# 1) Region
df_reg = pd.read_csv("outputs/avg_delay_by_region.csv")
plt.figure(figsize=(8,6))
sns.barplot(data=df_reg, x="mean_delay", y="region", palette="magma")
plt.title("Average Delay by Region")
plt.xlabel("Mean Delay (min)")
plt.ylabel("Region (ISO Code)")
plt.tight_layout()
plt.savefig("outputs/plots/bar_delay_by_region.png")
plt.show()

# 2) Volume Bucket
df_buck = pd.read_csv("outputs/avg_delay_by_bucket.csv")
plt.figure(figsize=(6,4))
sns.barplot(data=df_buck, x="bucket", y="mean_delay", order=["Low","Med","High"], palette="viridis")
plt.title("Average Delay by Flight‐Count Bucket")
plt.xlabel("Volume Bucket")
plt.ylabel("Mean Delay (min)")
plt.tight_layout()
plt.savefig("outputs/plots/bar_delay_by_bucket.png")
plt.show()

# 3) Weekday
df_dow = pd.read_csv("outputs/avg_delay_by_weekday.csv")
# ensure Monday→Sunday order
weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
df_dow["dayOfWeek"] = pd.Categorical(df_dow.dayOfWeek, categories=weekday_order, ordered=True)
df_dow = df_dow.sort_values("dayOfWeek")
plt.figure(figsize=(8,4))
sns.barplot(data=df_dow, x="dayOfWeek", y="mean_delay", palette="rocket")
plt.title("Average Delay by Day of Week")
plt.xlabel("Day of Week")
plt.ylabel("Mean Delay (min)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("outputs/plots/bar_delay_by_weekday.png")
plt.show()
