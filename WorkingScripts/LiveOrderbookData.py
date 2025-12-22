import json
import threading
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
from websocket import WebSocketApp

# ============ CONFIGURATION ============
# Get these from the Gamma API or the URL slug
TOKEN_ID_UP = "85405398028343230478158472254701676422933984902308782252271836354625808261213"      # REPLACE WITH ACTUAL TOKEN ID
TOKEN_ID_DOWN = "102403134145711160308610936179070371492516617389179678344710406630927087880128"    # REPLACE WITH ACTUAL TOKEN ID
MARKET_SLUG = "btc-updown-15m-1766363400"
MARKET_END_UNIX = 1766363400 + 900

# File Storage
DATA_DIR = Path("polymarket_data")
DATA_DIR.mkdir(exist_ok=True)
FILENAME = DATA_DIR / "orderbook_snapshots.parquet"

# ============ SHARED STATE ============
books = {
    TOKEN_ID_UP: {"bids": [], "asks": []},
    TOKEN_ID_DOWN: {"bids": [], "asks": []},
}
books_lock = threading.Lock()

# Buffer for saving data
snapshot_buffer = []
last_save_time = time.time()

# ============ WEBSOCKET: Live Orderbook ============
def on_open(ws):
    print("[WS] Connected")
    # Subscribe to BOTH Up and Down tokens
    msg = {
        "assets_ids": [TOKEN_ID_UP, TOKEN_ID_DOWN], 
        "type": "market"
    }
    ws.send(json.dumps(msg))

def on_message(ws, message):
    if message == "PONG": return
    
    try:
        data = json.loads(message)
        # Handle both single events and lists of events
        events = data if isinstance(data, list) else [data]

        for event in events:
            if event.get("event_type") != "book":
                continue

            asset_id = event.get("asset_id")
            if asset_id not in books:
                continue

            # CRITICAL: Polymarket sends snapshots. Overwrite the list.
            with books_lock:
                # API sends strings often, keep them as is or cast to float later
                books[asset_id]["bids"] = event.get("bids") or []
                books[asset_id]["asks"] = event.get("asks") or []
                
    except Exception as e:
        print(f"[WS ERROR] {e}")

def run_websocket():
    # Loop to auto-reconnect if internet drops
    while True:
        try:
            ws = WebSocketApp(
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                on_open=on_open,
                on_message=on_message
            )
            ws.run_forever()
            print("[WS] Reconnecting in 2s...")
            time.sleep(2)
        except Exception as e:
            print(f"[WS CRASH] {e}")
            time.sleep(5)

# ============ SAMPLER & SAVER ============
def get_depth_metrics(levels, n=5):
    """
    Calculates Weighted Avg Price, Total Size, and Best Price for top N levels.
    """
    if not levels:
        return None, 0.0, 0.0
    
    # Polymarket levels are ["price", "size"] strings. Convert to float.
    # Note: Bids/Asks are pre-sorted by the server (Best is usually first or last).
    # We assume standard formatting: Bids[0] is best, Asks[0] is best.
    # If using 'pop', check index. Here we iterate top N.
    
    total_size = 0.0
    weighted_sum = 0.0
    best_price = float(levels[0]["price"])
    
    count = 0
    for level in levels:
        p = float(level["price"])
        s = float(level["size"])
        weighted_sum += p * s
        total_size += s
        count += 1
        if count >= n:
            break
            
    avg_price = weighted_sum / total_size if total_size > 0 else 0.0
    return best_price, total_size, avg_price

def sampler_and_saver():
    global snapshot_buffer, last_save_time
    print("[SAMPLER] Started...")
    
    while True:
        now_ts = time.time()
        
        # 1. SAMPLE THE STATE
        with books_lock:
            # Copy state to avoid holding lock during calculations
            up_bids = list(books[TOKEN_ID_UP]["bids"])
            up_asks = list(books[TOKEN_ID_UP]["asks"])
            down_bids = list(books[TOKEN_ID_DOWN]["bids"])
            down_asks = list(books[TOKEN_ID_DOWN]["asks"])

        # Only record if we have data
        if up_bids and up_asks:
            # Calculate Metrics (Up Token)
            up_bid_px, up_bid_sz, _ = get_depth_metrics(up_bids)
            up_ask_px, up_ask_sz, _ = get_depth_metrics(up_asks)
            
            # Calculate Metrics (Down Token)
            down_bid_px, down_bid_sz, _ = get_depth_metrics(down_bids)
            down_ask_px, down_ask_sz, _ = get_depth_metrics(down_asks)

            # Create Record
            record = {
                "timestamp": datetime.fromtimestamp(now_ts),
                "unixtime": now_ts,
                "market_slug": MARKET_SLUG,
                
                # Up Token
                "up_best_bid": up_bid_px,
                "up_best_ask": up_ask_px,
                "up_bid_depth_5": up_bid_sz,
                "up_ask_depth_5": up_ask_sz,
                "up_spread": up_ask_px - up_bid_px,
                
                # Down Token
                "down_best_bid": down_bid_px,
                "down_best_ask": down_ask_px,
                "down_spread": down_ask_px - down_bid_px,
                
                # Implied Probability Check (Should sum to ~$1.00)
                "implied_sum_bid": up_bid_px + down_bid_px,
                "implied_sum_ask": up_ask_px + down_ask_px
            }
            snapshot_buffer.append(record)

        # 2. SAVE TO DISK (Every 60s or 1000 records)
        if len(snapshot_buffer) > 0 and (now_ts - last_save_time > 60 or len(snapshot_buffer) >= 1000):
            print(f"💾 Flushing {len(snapshot_buffer)} orderbook snapshots...")
            
            try:
                new_df = pd.DataFrame(snapshot_buffer)
                
                if FILENAME.exists():
                    existing_df = pd.read_parquet(FILENAME)
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    final_df = new_df
                
                final_df.to_parquet(FILENAME, index=False)
                print("✅ Saved.")
                
                snapshot_buffer = []
                last_save_time = now_ts
                
            except Exception as e:
                print(f"⚠️ Save Failed: {e}")

        # Sample rate (0.1s = 10Hz)
        time.sleep(0.1)

# ============ EXECUTION ============
if __name__ == "__main__":
    # 1. Start WebSocket Thread
    t_ws = threading.Thread(target=run_websocket, daemon=True)
    t_ws.start()
    
    # 2. Start Sampler/Saver (Main Thread)
    # We run this on the main thread so Ctrl+C works nicely
    try:
        sampler_and_saver()
    except KeyboardInterrupt:
        print("Stopping...")
        # Save remaining data before exit
        if snapshot_buffer:
            print("Saving final data...")
            pd.DataFrame(snapshot_buffer).to_parquet(FILENAME, index=False)