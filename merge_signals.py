import pandas as pd

# -------- CONFIG --------
SIGNALS_FILE = "data/signals.csv"
SNAPSHOTS_FILE = "data/signals_snapshots.csv"
OUTPUT_FILE = "data/signals_merged.csv"


try:
    # -------- LOAD DATA --------
    signals = pd.read_csv(SIGNALS_FILE)
    snapshots = pd.read_csv(SNAPSHOTS_FILE)

    # -------- PIVOT SNAPSHOTS --------
    snapshots_pivot = (
        snapshots
        .pivot_table(
            index=["signal_id", "market_id"],
            columns="snapshot_type",
            values=["best_bid", "best_ask", "yes_price"],
            aggfunc="last"
        )
    )

    # Flatten multi-level columns
    snapshots_pivot.columns = [
        f"{metric}_{snapshot_type}"
        for metric, snapshot_type in snapshots_pivot.columns
    ]

    snapshots_pivot = snapshots_pivot.reset_index()

    # -------- MERGE --------
    merged = signals.merge(
        snapshots_pivot,
        on=["signal_id", "market_id"],
        how="left"
    )

    # -------- SAVE --------
    merged.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Merged file saved as: {OUTPUT_FILE}")
except:
    print("Unable to merge signals")