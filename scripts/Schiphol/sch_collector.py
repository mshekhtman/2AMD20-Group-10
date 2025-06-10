#!/usr/bin/env python3
"""
schiphol_collector.py

Fetches raw flight and reference data from the Schiphol PublicFlights API
for a given date range (departures only), and saves to JSON files.

Usage:
  # Fetch departures between two dates:
  python scripts/Schiphol/sch_collector.py \
      --start-date 2024-01-01 \
      --end-date   2024-03-31 \
      --config     config/schiphol_api.json

  # Fetch reference data (airlines, destinations, aircraft types):
  python scripts/Schiphol/sch_collector.py --fetch-ref --config config/schiphol_api.json
"""


class KLMHistoricalCollector:
    def __init__(self, cfg_path):
        cfg = json.load(open(cfg_path))
        self.base = "https://api.airfranceklm.com/opendata/flightstatus/v4/flights"
        self.headers = {
          "Content-Type":"application/json",
          "x-api-key": cfg["api_key"]
        }

    def fetch_q1_2024(self):
        # Example: get daily flight statuses for one date
        results = []
        for d in ["2024-01-01", "2024-01-02", /*…*/]:
            params = {"departureDate": d, "carrierCode":"KL"} 
            r = requests.get(self.base, headers=self.headers, params=params)
            if r.status_code == 200:
                results.extend(r.json().get("data", []))
        out = f"klm_q1_2024_{datetime.utcnow():%Y%m%d}.json"
        json.dump(results, open(out,"w"), indent=2)
        print("Saved", len(results))

import os
import json
import time
import logging
import argparse
import requests
from datetime import datetime, timedelta

class SchipholDataCollector:
    def __init__(self, api_config_path: str):
        # Load API credentials and base URL
        with open(api_config_path, 'r') as f:
            cfg = json.load(f)
        self.base_url = cfg["base_url"].rstrip('/')   # e.g. "https://api.schiphol.nl/public-flights"
        self.app_id   = cfg["app_id"]
        self.app_key  = cfg["app_key"]

        # Endpoints
        self.flights_ep  = f"{self.base_url}/flights"
        self.airlines_ep = f"{self.base_url}/airlines"
        self.dest_ep     = f"{self.base_url}/destinations"
        self.aircraft_ep = f"{self.base_url}/aircraftTypes"

        # Headers
        self.headers = {
            "Accept":          "application/json",
            "ResourceVersion": "v4",
            "app_id":          self.app_id,
            "app_key":         self.app_key
        }
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s'
        )
        logging.info("Initialized SchipholDataCollector")

    def fetch_flights(self, start_date: str, end_date: str) -> str:
        """
        Fetch all departures 2024-01-01 → 2024-03-31 by:
         1) GET /flights?flightDirection=D&scheduleDate=YYYY-MM-DD
         2) Paging via Link headers until no more `rel="next"`
        Saves combined JSON to data/Schiphol/raw/.
        """
        date_fmt = "%Y-%m-%d"
        start_dt = datetime.strptime(start_date, date_fmt)
        end_dt   = datetime.strptime(end_date,   date_fmt)

        all_flights = []
        current     = start_dt

        while current <= end_dt:
            day = current.strftime(date_fmt)
            logging.info(f"Fetching departures for {day}")

            url    = self.flights_ep
            params = {"flightDirection": "D", "scheduleDate": day}

            # page through all pages for this day
            while True:
                resp = requests.get(url, headers=self.headers, params=params)
                if resp.status_code != 200:
                    logging.error(f"[{day}] HTTP {resp.status_code}: {resp.text}")
                    break

                data    = resp.json()
                flights = data.get("flights", [])
                all_flights.extend(flights)
                logging.info(f"[{day}] got {len(flights)} flights")

                # pagination
                link = resp.headers.get("Link", "")
                next_url = None
                for part in link.split(","):
                    if 'rel="next"' in part:
                        next_url = part.split(";")[0].strip("<> ")
                        break
                if not next_url:
                    break

                url    = next_url
                params = None
                time.sleep(0.2)

            current += timedelta(days=1)
            time.sleep(0.3)

        # write out
        out_dir = os.path.join("data", "Schiphol", "raw")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out = os.path.join(out_dir, f"flights_{start_date}_{end_date}_{ts}.json")
        with open(out, "w") as f:
            json.dump({"flights": all_flights}, f, indent=2)

        logging.info(f"Saved {len(all_flights)} flights to {out}")
        return out


    def fetch_reference_data(self) -> None:
        """
        Fetches reference endpoints: airlines, destinations, and aircraft types.
        Saves each to its own JSON file under data/Schiphol/raw/.
        """
        endpoints = {
            "airlines":     self.airlines_ep,
            "destinations": self.dest_ep,
            "aircraft":     self.aircraft_ep
        }
        out_dir = os.path.join("data", "Schiphol", "raw")
        os.makedirs(out_dir, exist_ok=True)

        for name, url in endpoints.items():
            logging.info(f"Fetching reference data: {name}")
            resp = requests.get(url, headers=self.headers)
            if resp.status_code != 200:
                logging.error(f"Failed to fetch {name} (HTTP {resp.status_code})")
                continue
            payload = resp.json()
            data = payload.get(name, payload)
            path = os.path.join(out_dir, f"{name}.json")
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            logging.info(f"Saved {len(data)} records to {path}")
            time.sleep(0.2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect Schiphol flight and reference data")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD) for departures", required=False)
    parser.add_argument("--end-date",   help="End date (YYYY-MM-DD) for departures", required=False)
    parser.add_argument("--config",     help="Path to API config JSON", default="config/schiphol_api.json")
    parser.add_argument("--fetch-ref",  help="Fetch reference data instead of flights", action="store_true")

    args = parser.parse_args()
    collector = SchipholDataCollector(api_config_path=args.config)

    if args.fetch_ref:
        collector.fetch_reference_data()
    else:
        if not args.start_date or not args.end_date:
            parser.error("Both --start-date and --end-date are required unless --fetch-ref is used.")
        collector.fetch_flights(args.start_date, args.end_date)

    logging.info("Done.")
