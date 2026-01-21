import pandas as pd
import requests
import json
from datetime import timedelta
from pathlib import Path
from datetime import datetime, timezone

# =========================
# CONFIG
# =========================

API_BASE = "https://gamma-api.polymarket.com"

DATA_DIR = Path("data")
SIGNALS_FILE = DATA_DIR / "signals.csv"
SNAPSHOTS_FILE = DATA_DIR / "signals_snapshots.csv"

REQUEST_TIMEOUT = 10  # seconds

# =========================
# HELPERS
# =========================

def fetch_market_price(market_id: str) -> dict:
    """
    Fetch current market prices from Polymarket.
    Returns best_bid, best_ask, yes_price.
    """
    url = f"{API_BASE}/markets/{market_id}"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    m = r.json()

    outcome_prices = m.get("outcomePrices", [])
    
    return {
        "best_bid": m.get("bestBid"),
        "best_ask": m.get("bestAsk"),
        "yes_price": json.loads(outcome_prices)[0] if len(json.loads(outcome_prices)) > 0 else None
    }


# =========================
# CORE LOGIC
# =========================

def process_snapshots():
    if not SIGNALS_FILE.exists():
        print("No signals.csv found — nothing to do.")
        return

    signals = pd.read_csv(
        SIGNALS_FILE,
        parse_dates=["signal_time_utc"]
    )
    
    signals["signal_time_utc"] = pd.to_datetime(signals["signal_time_utc"], utc=True, errors="coerce")

    # Drop rows with invalid timestamps
    signals = signals.dropna(subset=["signal_time_utc"])

    
    # Current UTC time
    now = pd.Timestamp.now(tz="UTC")

    snapshots_to_write = []

    for idx, row in signals.iterrows():
        signal_time = row["signal_time_utc"]

        # ---------- 1H SNAPSHOT ----------
        if not row.get("snapshot_1h_done", False):
            if now >= signal_time + timedelta(hours=1):
                try:
                    price = fetch_market_price(row["market_id"])
                    snapshots_to_write.append({
                        "signal_id": row["signal_id"],
                        "market_id": row["market_id"],
                        "snapshot_type": "1h",
                        "snapshot_time_utc": now,
                        **price
                    })
                    signals.at[idx, "snapshot_1h_done"] = True
                except Exception as e:
                    print(f"[WARN] 1h snapshot failed for {row['market_id']}: {e}")

        # ---------- 24H SNAPSHOT ----------
        if not row.get("snapshot_24h_done", False):
            if now >= signal_time + timedelta(hours=24):
                try:
                    price = fetch_market_price(row["market_id"])
                    snapshots_to_write.append({
                        "signal_id": row["signal_id"],
                        "market_id": row["market_id"],
                        "snapshot_type": "24h",
                        "snapshot_time_utc": now,
                        **price
                    })
                    signals.at[idx, "snapshot_24h_done"] = True
                except Exception as e:
                    print(f"[WARN] 24h snapshot failed for {row['market_id']}: {e}")

    # =========================
    # WRITE RESULTS
    # =========================

    if snapshots_to_write:
        df_snapshots = pd.DataFrame(snapshots_to_write)

        SNAPSHOTS_FILE.parent.mkdir(parents=True, exist_ok=True)

        df_snapshots.to_csv(
            SNAPSHOTS_FILE,
            mode="a",
            header=not SNAPSHOTS_FILE.exists(),
            index=False
        )

        print(f"Wrote {len(df_snapshots)} snapshot rows.")

    # Persist updated flags
    signals.to_csv(SIGNALS_FILE, index=False)


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    print(f"Snapshot runner started at {datetime.now(timezone.utc).isoformat()}")
    process_snapshots()
    print("Snapshot runner finished.")
