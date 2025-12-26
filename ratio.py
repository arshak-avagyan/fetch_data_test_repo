import pandas as pd
import numpy as np
import requests
import ast
from dotenv import load_dotenv
import os

load_dotenv()  # loads .env locally, no effect in GitHub Actions

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID")

def merge_and_compute_change(t0_path="data/polymarket_flat_markets_t0.csv", t1_path="data/polymarket_flat_markets_t1.csv"):
    # Load CSVs
    df_t0 = pd.read_csv(t0_path)
    df_t1 = pd.read_csv(t1_path)

    # ---------- SAFE PARSER ----------
    def parse_outcome_prices(x):
        try:
            # If it's already a list of floats, just return
            if isinstance(x, list) and len(x) >= 2:
                return [float(x[0]), float(x[1])]
            # If it's a string list, parse it
            if isinstance(x, str):
                vals = ast.literal_eval(x)
                if isinstance(vals, list) and len(vals) >= 2:
                    return [float(vals[0]), float(vals[1])]
        except Exception:
            pass
        # default fallback
        return [np.nan, np.nan]

    # Parse outcome prices
    df_t0["market_outcomePrices"] = df_t0["market_outcomePrices"].apply(parse_outcome_prices)
    df_t1["market_outcomePrices"] = df_t1["market_outcomePrices"].apply(parse_outcome_prices)

    # Split outcomes into columns
    df_t0[["outcome_1_t0", "outcome_2_t0"]] = pd.DataFrame(
        df_t0["market_outcomePrices"].tolist(), index=df_t0.index
    )
    df_t1[["outcome_1", "outcome_2"]] = pd.DataFrame(
        df_t1["market_outcomePrices"].tolist(), index=df_t1.index
    )

    # ---------- MERGE ----------
    df_merged = df_t1.merge(
        df_t0[
            [
                "market_id",
                "market_volume",
                "market_bestBid",
                "market_bestAsk",
                "outcome_1_t0",
                "outcome_2_t0",
            ]
        ],
        on="market_id",
        how="left",
        suffixes=("", "_t0"),
    )

    # ---------- % CHANGE ----------
    def pct_change(new, old):
        return ((new - old) / old) * 100
    
    def diff_change(new, old):
        return (new - old).where((new > 0) & (new < 1) & (old > 0) & (old < 1), 0)


    df_merged["outcome_1_change"] = diff_change(
        df_merged["outcome_1"], df_merged["outcome_1_t0"]
    )

    df_merged["outcome_2_change"] = diff_change(
        df_merged["outcome_2"], df_merged["outcome_2_t0"]
    )

    df_merged["best_bid_change"] = diff_change(
        df_merged["market_bestBid"], df_merged["market_bestBid_t0"]
    )

    df_merged["best_ask_change"] = diff_change(
        df_merged["market_bestAsk"], df_merged["market_bestAsk_t0"]
    )

    # Handle new markets / divide-by-zero
    df_merged[["outcome_1_change", "outcome_2_change", "best_bid_change", "best_ask_change"]] = (
        df_merged[["outcome_1_change", "outcome_2_change", "best_bid_change", "best_ask_change"]]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    return df_merged


# Example usage
df_master = merge_and_compute_change()
df_master.to_csv("data/market_change.csv", index = False)




class VolumeAlertService:
    def __init__(self, telegram_token: str, chat_id: str, bid_threshold: float=20, ask_threshold: float=20):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        self.bid_threshold=bid_threshold
        self.ask_threshold=ask_threshold

    def send_message(self, text: str):
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        response = requests.post(self.api_url, json=payload)
        response.raise_for_status()
    
    # def build_price_change_message(self, row):
    #     bid_change = row.get("market_bestBid_pct_change", 0)
    #     ask_change = row.get("market_bestAsk_pct_change", 0)
    #     # price_change = row.get("outcome_1_change", 0)
        
    #     messages = []

    #     def format_msg(label, change):
    #         direction = "up" if change > 0 else "down"
    #         arrow = "🟩" if change > 0 else "🔻"
    #         pct = abs(round(change, 2))
    #         return f"{arrow} {label} price went {direction} by {pct}%"

    #     # Bid change
    #     if abs(bid_change) >= self.bid_threshold:
    #         messages.append(format_msg("Bid", bid_change))

    #     # Ask change
    #     if abs(ask_change) >= self.ask_threshold:
    #         messages.append(format_msg("Ask", ask_change))

    #     # If both changed → separate sentences
    #     return "\n".join(messages)

    def build_price_change_message(self, row):
        price_change = row.get("outcome_1_change", 0)
        
        messages = []

        def format_msg(change):
            direction = "up" if change > 0 else "down"
            arrow = "🟩" if change > 0 else "🔻"
            pct = abs(round(change * 100, 2))
            return f"{arrow} Price went {direction} by {pct} percentage points"

        # Bid change
        # if abs(bid_change) >= self.bid_threshold:
        #     messages.append(format_msg("Bid", bid_change))

        # Ask change
        # if abs(ask_change) >= self.ask_threshold:
        #     messages.append(format_msg("Ask", ask_change))

        # If both changed → separate sentences
        messages.append(format_msg(price_change))
        return "\n".join(messages)

    def build_message(self, row):
        event_slug = row.get("event_slug", "")
        link = f"https://polymarket.com/event/{event_slug}"

        #Get Bid/Ask movement message
        price_change_msg = self.build_price_change_message(row)

        # Safely format end date
        end_date = pd.to_datetime(row['market_endDate'], errors='coerce')
        end_date_str = end_date.strftime('%m/%d/%Y') if pd.notna(end_date) else "N/A"

        message = (
            "<b>🔥 Market Update</b>\n\n"
            f"{price_change_msg}\n\n"
            f"<b>Question:</b> {row['market_question']}\n\n"
            f"💹 <b>T1 Best Bid / Ask:</b> {row['market_bestBid']} / {row['market_bestAsk']}\n"
            f"💹 <b>T0 Best Bid / Ask:</b> {row['market_bestBid_t0']} / {row['market_bestAsk_t0']}\n"
            f"📊 <b>Market Volume:</b> {round(row['market_volume']):,}\n"
            f"⏰ <b>Ends:</b> {end_date_str}\n\n"
            f"T0 Price: {row['outcome_1_t0']} | T1 Price: {row['outcome_1']}\n\n"
            # "\n\n"
            f"🔗 <b>Event:</b> {link}\n\n"
            
            # f"T0 Bid Price: {row['market_bestBid_t0']} | T1 Bid Price: {row['market_bestBid']}\n\n"
            # f"T0 Ask Price: {row['market_bestAsk_t0']} | T1 Ask Price: {row['market_bestAsk']}\n\n"
            
        )

        return message

    def process_dataframe(self, df):
        # alert_rows = df[
        #     (df["market_closed"] == False) &
        #     (df["market_active"] == True) &
        #     (df["outcome_1_change"].abs() >= 0.2)
            # (
            #     (df["market_bestBid_pct_change"].abs() >= self.bid_threshold) |
            #     (df["market_bestAsk_pct_change"].abs() >= self.ask_threshold)
            # )
        # ]


        for _, row in df.iterrows():
            msg = self.build_message(row)
            self.send_message(msg)



# Tags to exclude
exclude_tags = [
    'nba',
    'nba-champion',
    'nba-finals',
    'ncaa-football',
    'soccer',
    'hockey',
    'football',
    'champions-league',
    '2026-fifa-world-cup',
    'sports',
    'nfl'
]

df = pd.read_csv("data/market_change.csv")
df["event_endDate"] = pd.to_datetime(df["event_endDate"], utc=True, errors="coerce")
now_utc = pd.Timestamp.utcnow()


mask = ~df["tags"].fillna("").apply(
    lambda x: bool(set(exclude_tags).intersection(x.split(",")))
)

df = df[mask]

df = df[
    (df["market_closed"] == False) &
    (df["market_active"] == True) &
    (df["event_endDate"] >= now_utc) &
    (
        (df["outcome_1_change"].abs() >= 0.2) |
        (df["outcome_2_change"].abs() >= 0.2) |
        (df["best_bid_change"].abs() >= 0.2) |
        (df["best_ask_change"].abs() >= 0.2)
        # (df["market_bestBid_pct_change"].abs() >= 20) |
        # (df["market_bestAsk_pct_change"].abs() >= 20) 
    )
]
# display(df[:5])
# Initialize service
service = VolumeAlertService(
    telegram_token=TELEGRAM_TOKEN,
    chat_id=GROUP_CHAT_ID
    # bid_threshold=20,
    # ask_threshold=20
)


service.process_dataframe(df)