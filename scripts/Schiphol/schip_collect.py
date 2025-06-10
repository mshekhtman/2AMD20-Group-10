#!/usr/bin/env python3
import os
import json
import requests
from datetime import datetime, timedelta

class KLMHistoricalCollector:
    def __init__(self, cfg_path: str):
        """
        Expects a JSON config with {"api_key": "YOUR_X-API-KEY"}.
        """
        cfg = json.load(open(cfg_path))
        self.base_url = "https://api.airfranceklm.com/opendata/flightstatus/v4/flights"
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key":    cfg["api_key"]
        }

    def fetch_q1_2024(self, output_dir: str = "data/KLM/raw") -> str:
        """
        Pulls flightstatus data for all KL departures from AMS
        between 2024-01-01 and 2024-03-31 (inclusive).
        Handles pagination via page/size & pageContext.totalPages.
        Saves to JSON and returns the file path.
        """
        # ensure output exists
        os.makedirs(output_dir, exist_ok=True)

        start_date = datetime(2024, 1, 1)
        end_date   = datetime(2024, 3, 31)
        cursor     = start_date

        all_flights = []

        while cursor <= end_date:
            day_str = cursor.strftime("%Y-%m-%d")
            print(f"► Fetching {day_str}")
            page      = 0
            page_size = 100  # API maximum per request

            while True:
                params = {
                    "carrierCode":           "KL",
                    "departureDate":         day_str,
                    "departureAirportCode":  "AMS",
                    "page":                  page,
                    "size":                  page_size
                }
                resp = requests.get(self.base_url, headers=self.headers, params=params)
                if resp.status_code != 200:
                    print(f"  ✖ {day_str} page {page} → HTTP {resp.status_code}: {resp.text}")
                    break

                payload      = resp.json()
                batch        = payload.get("data", [])
                all_flights.extend(batch)
                print(f"  ✔ Retrieved {len(batch)} records (page {page})")

                # Pagination info
                pc           = payload.get("pageContext", {})
                total_pages  = pc.get("totalPages", 1)
                if page + 1 >= total_pages:
                    break
                page += 1

            # next calendar day
            cursor += timedelta(days=1)

        # write out full Q1-2024 bulk
        ts     = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_fp = os.path.join(output_dir, f"klm_q1_2024_{ts}.json")
        with open(out_fp, "w") as f:
            json.dump(all_flights, f, indent=2)
        print(f"✨ Saved total {len(all_flights)} records to {out_fp}")
        return out_fp


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect Q1-2024 KLM flightstatus from AMS"
    )
    parser.add_argument(
        "--config",
        help="Path to KLM API config JSON (with api_key)",
        default="config/klm_api.json"
    )
    parser.add_argument(
        "--out-dir",
        help="Where to save raw JSON",
        default="data/KLM/raw"
    )
    args = parser.parse_args()

    collector = KLMHistoricalCollector(cfg_path=args.config)
    collector.fetch_q1_2024(output_dir=args.out_dir)