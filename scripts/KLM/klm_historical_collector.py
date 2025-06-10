#!/usr/bin/env python3
import os
import json
import time
import base64
import requests
from datetime import datetime, timedelta, timezone

class KLMHistoricalCollector:
    def __init__(self, cfg_path: str):
        """
        Expects config JSON with keys:
          - api_key, api_secret
          - base_url, token_url
          - endpoints.flights_v4
          - rate_limits.calls_per_second
        """
        cfg = json.load(open(cfg_path))
        self.api_key       = cfg["api_key"]
        self.api_secret    = cfg["api_secret"]
        self.base_url      = cfg["base_url"].rstrip('/')
        self.token_url     = cfg["token_url"]
        self.flights_ep    = self.base_url + cfg["endpoints"]["flights_v4"]
        self.rate_per_sec  = cfg.get("rate_limits", {}).get("calls_per_second", 1)
        self._token        = None
        self._token_expiry = None

    def _get_token(self) -> str:
        """Fetch (and cache) an OAuth2 Bearer token, with retries."""
        # Return cached if still valid
        now = datetime.now(timezone.utc)
        if self._token and self._token_expiry and now < self._token_expiry:
            return self._token


        # Prepare Basic auth header
        creds = f"{self.api_key}:{self.api_secret}".encode()
        auth = base64.b64encode(creds).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type":  "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}

        # Exponential backoff retry loop
        for attempt in range(1, 4):
            resp = requests.post(self.token_url, headers=headers, data=data)
            if resp.status_code == 200:
                body = resp.json()
                self._token = body["access_token"]
                expires = body.get("expires_in", 3600)
                # subtract a minute for safety
                self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires - 60)
                return self._token

            # on 4xx, no retry
            if 400 <= resp.status_code < 500:
                break

            # on 5xx or other, back off and retry
            sleep = 2 ** attempt
            print(f"Token fetch failed (HTTP {resp.status_code}), retry in {sleep}s")
            time.sleep(sleep)

        # If we get here, we couldn't fetch a valid token
        raise RuntimeError(
            f"Failed to retrieve OAuth2 token after retries: "
            f"{resp.status_code} {resp.text}"
        )

    def fetch_q1_2024(self, output_dir: str = "data/KLM/raw") -> str:
        """
        Retrieves all KL departures from AMS in Q1-2024 via flightstatus/v4/flights,
        handling pagination and rate-limits.
        """
        os.makedirs(output_dir, exist_ok=True)
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json"
        }

        start_dt = datetime(2024, 1, 1)
        end_dt   = datetime(2024, 3, 31)
        cursor   = start_dt
        results  = []

        while cursor <= end_dt:
            day_str = cursor.strftime("%Y-%m-%d")
            print(f"► {day_str}")

            page = 0
            while True:
                params = {
                    "carrierCode":          "KL",
                    "departureDate":        day_str,
                    "departureAirportCode": "AMS",
                    "page":                 page,
                    "size":                 100
                }
                resp = requests.get(self.flights_ep, headers=headers, params=params)
                if resp.status_code == 403:
                    raise RuntimeError(
                        "403 Forbidden: Check that your KLM Developer app is active."
                    )
                if resp.status_code != 200:
                    print(f"  ✖ HTTP {resp.status_code}: {resp.text}")
                    break

                payload     = resp.json()
                batch       = payload.get("data", [])
                results    .extend(batch)
                print(f"  ✔ {len(batch)} records (page {page})")

                pc          = payload.get("pageContext", {})
                total_pages = pc.get("totalPages", 1)
                if page + 1 >= total_pages:
                    break
                page += 1
                time.sleep(1 / self.rate_per_sec)

            cursor += timedelta(days=1)
            time.sleep(1 / self.rate_per_sec)

        ts     = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_fp = os.path.join(output_dir, f"klm_q1_2024_{ts}.json")
        with open(out_fp, "w") as f:
            json.dump(results, f, indent=2)
        print(f"✨ Saved {len(results)} records → {out_fp}")
        return out_fp

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(
        description="Fetch Q1-2024 KLM flightstatus for AMS departures"
    )
    p.add_argument(
        "--config", default="config/klm_api.json",
        help="Path to KLM API config JSON"
    )
    p.add_argument(
        "--out-dir", default="data/KLM/raw",
        help="Directory to save raw JSON"
    )
    args = p.parse_args()

    collector = KLMHistoricalCollector(cfg_path=args.config)
    collector.fetch_q1_2024(output_dir=args.out_dir)
