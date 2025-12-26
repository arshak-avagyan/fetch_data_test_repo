import requests
import time
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path



API_BASE = "https://gamma-api.polymarket.com"

def fetch_events(limit=100, offset=0, min_volume=1000000):
    url = (
        f"{API_BASE}/events?"
        f"limit={limit}&offset={offset}&volume_min={min_volume}&active=true&closed=false"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_all_events(min_volume=1000000):
    all_events = []
    offset = 0
    limit = 100

    while offset < 600:
        print(f"Fetching offset={offset} ...")

        data = fetch_events(limit=limit, offset=offset, min_volume=min_volume)

        if not data:
            break

        all_events.extend(data)
        offset += limit

        time.sleep(5)

    return all_events


def flatten_events(events):
    event_fields = [
        "id", "slug", "title", "creationDate", "endDate", "startDate", "volume"
    ]

    market_fields = [
        "id", "slug", "question", "startDate", "endDate", "volume", 
        "outcomes", "outcomePrices", "active", "closed", "bestBid", "bestAsk"
    ]

    rows = []

    for event in events:
        event_data = {f"event_{k}": event.get(k) for k in event_fields}

        # --- event-level tags ---
        event_tags = [
            t.get("slug")
            for t in event.get("tags", [])
            if isinstance(t, dict) and t.get("slug")
        ]

        for market in event.get("markets", []):
            market_data = {f"market_{k}": market.get(k) for k in market_fields}

            # --- market-level tags ---
            market_tags = [
                t.get("slug")
                for t in market.get("tags", [])
                if isinstance(t, dict) and t.get("slug")
            ]

            # merge & deduplicate tags
            all_tags = sorted(set(event_tags + market_tags))
            

            row = {
                "market_id": market.get("id"),
                "tags": ",".join(all_tags),
                **event_data,
                **market_data
            }
            rows.append(row)

    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    df['created_at'] = datetime.now(timezone.utc).replace(microsecond=0).isoformat(timespec='seconds').replace('+00:00', '')

    return df

def main():
    print("Fetching Polymarket events...")
    events = fetch_all_events()
    print(f"Fetched {len(events)} events.")

    # Flatten markets
    df = flatten_events(events)

    old_file = Path("data/polymarket_flat_markets_t0.csv")
    new_file = Path("data/polymarket_flat_markets_t1.csv")

    try:
        # Rename old t1 to t0 (overwrite if exists)
        if new_file.exists():
            new_file.replace(old_file)  # replace() overwrites t0 if it exists
            print(f"Renamed {new_file.name} to {old_file.name}")

        # Save the new CSV as t1
        df.to_csv(new_file, index=False)
        print(f"Saved {len(df)} rows to {new_file.name}")

    except Exception as e:
        print(f"Error occurred: {e}. Saving backup...")
        backup_file = Path("data/polymarket_flat_markets_backup.csv")
        df.to_csv(backup_file, index=False)
        print(f"Saved backup to {backup_file.name}")

main()