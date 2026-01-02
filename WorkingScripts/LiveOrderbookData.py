import json
import threading
import pandas as pd
import time
import signal
import sys
import atexit
import requests
from datetime import datetime
from pathlib import Path
from websocket import WebSocketApp

# ============ CONFIGURATION ============
TOKEN_ID_UP = "53398642787529207982308002576599779120889686331928151735250039510047392627488"
TOKEN_ID_DOWN = "29822429672172354396578021183134197938504920225831222333842740602723777494319"
MARKET_SLUG = "btc-updown-15m-1766692800"

DATA_DIR = Path("DataCollection/OrderbookData")
DATA_DIR.mkdir(exist_ok=True)
FILENAME = DATA_DIR / "1766692800-orderbook.parquet"

# Sampling and save settings
SAMPLE_INTERVAL = 0.1  # seconds between samples
SAVE_INTERVAL = 60     # seconds between saves
MAX_BUFFER_SIZE = 1000 # force save after this many records

# REST API endpoint (confirmed to return accurate prices)
CLOB_API = "https://clob.polymarket.com"

# ============ SHARED STATE ============
prices = {
    TOKEN_ID_UP: {"best_bid": None, "best_ask": None, "last_trade": None},
    TOKEN_ID_DOWN: {"best_bid": None, "best_ask": None, "last_trade": None},
}
data_lock = threading.Lock()
snapshot_buffer = []
last_save_time = time.time()
running = True
ws_instance = None

# ============ REST API PRICE FETCHER ============
def fetch_price(token_id, side):
    """Fetch price from REST API (confirmed accurate)."""
    try:
        resp = requests.get(
            f"{CLOB_API}/price",
            params={"token_id": token_id, "side": side},
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            return float(data.get("price", 0))
    except:
        pass
    return None

def price_poller():
    """Poll REST API for accurate prices."""
    while running:
        try:
            for token_id in [TOKEN_ID_UP, TOKEN_ID_DOWN]:
                bid = fetch_price(token_id, "BUY")   # Best bid = what buyers offer (you receive when selling)
                ask = fetch_price(token_id, "SELL")  # Best ask = what sellers want (you pay when buying)
                
                if bid is not None and ask is not None:
                    with data_lock:
                        prices[token_id]["best_bid"] = bid
                        prices[token_id]["best_ask"] = ask
            
            time.sleep(0.5)  # Poll every 500ms
        except:
            time.sleep(1)

# ============ DATA PERSISTENCE ============
def save_buffer_to_disk(final=False):
    """Save buffered snapshots to parquet file."""
    global snapshot_buffer, last_save_time
    
    if not snapshot_buffer:
        return
    
    try:
        new_df = pd.DataFrame(snapshot_buffer)
        
        if FILENAME.exists():
            existing_df = pd.read_parquet(FILENAME)
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        
        final_df.to_parquet(FILENAME, index=False)
        
        count = len(snapshot_buffer)
        snapshot_buffer = []
        last_save_time = time.time()
        
        if final:
            print(f"[FINAL SAVE] {count} records saved to {FILENAME}")
        
    except Exception as e:
        print(f"[SAVE ERROR] {e}")

def graceful_shutdown(*args):
    """Handle graceful shutdown on signal or exit."""
    global running, ws_instance
    
    running = False
    
    if ws_instance:
        try:
            ws_instance.close()
        except:
            pass
    
    save_buffer_to_disk(final=True)
    
    total_records = 0
    if FILENAME.exists():
        try:
            df = pd.read_parquet(FILENAME)
            total_records = len(df)
        except:
            pass
    
    print(f"[SHUTDOWN] Total records in file: {total_records}")
    sys.exit(0)

# Register shutdown handlers
signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)
atexit.register(lambda: save_buffer_to_disk(final=True))

# ============ WEBSOCKET (for last_trade_price) ============
def on_open(ws):
    global ws_instance
    ws_instance = ws
    
    print(f"[CONNECTED] Subscribing to market: {MARKET_SLUG}")
    
    msg = {
        "assets_ids": [TOKEN_ID_UP, TOKEN_ID_DOWN],
        "type": "market"
    }
    ws.send(json.dumps(msg))
    
    def ping():
        while running:
            try:
                ws.send("PING")
                time.sleep(10)
            except:
                break
    threading.Thread(target=ping, daemon=True).start()

def on_message(ws, message):
    if message == "PONG":
        return

    try:
        data = json.loads(message)
        events = data if isinstance(data, list) else [data]

        for event in events:
            event_type = event.get("event_type")
            
            # Capture last trade price from WebSocket (this is accurate)
            if event_type == "last_trade_price":
                asset_id = event.get("asset_id")
                if asset_id in prices:
                    with data_lock:
                        prices[asset_id]["last_trade"] = float(event.get("price", 0))

    except:
        pass

def on_close(ws, close_status_code, close_msg):
    print(f"[DISCONNECTED] Code: {close_status_code}")
    save_buffer_to_disk(final=True)

def on_error(ws, error):
    pass

def run_websocket():
    while running:
        try:
            ws = WebSocketApp(
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                on_open=on_open,
                on_message=on_message,
                on_close=on_close,
                on_error=on_error
            )
            ws.run_forever()
            
            if running:
                save_buffer_to_disk()
                time.sleep(2)
        except:
            if running:
                time.sleep(5)

# ============ SAMPLER & SAVER ============
def sampler_and_saver():
    global snapshot_buffer, last_save_time
    
    print(f"[STARTED] Capturing to {FILENAME}")
    print(f"[CONFIG] Sample interval: {SAMPLE_INTERVAL}s, Save interval: {SAVE_INTERVAL}s")
    
    records_since_last_log = 0
    last_log_time = time.time()

    while running:
        now_ts = time.time()

        with data_lock:
            up_bid = prices[TOKEN_ID_UP]["best_bid"]
            up_ask = prices[TOKEN_ID_UP]["best_ask"]
            up_last = prices[TOKEN_ID_UP]["last_trade"]
            down_bid = prices[TOKEN_ID_DOWN]["best_bid"]
            down_ask = prices[TOKEN_ID_DOWN]["best_ask"]
            down_last = prices[TOKEN_ID_DOWN]["last_trade"]

        if up_bid is not None and up_ask is not None:
            record = {
                "timestamp": datetime.fromtimestamp(now_ts),
                "unixtime": now_ts,
                "market_slug": MARKET_SLUG,
                "up_best_bid": up_bid,
                "up_best_ask": up_ask,
                "up_spread": up_ask - up_bid,
                "up_last_trade": up_last,
                "down_best_bid": down_bid,
                "down_best_ask": down_ask,
                "down_spread": (down_ask - down_bid) if down_bid and down_ask else None,
                "down_last_trade": down_last,
                "implied_sum_bid": up_bid + (down_bid or 0),
                "implied_sum_ask": up_ask + (down_ask or 0),
            }
            snapshot_buffer.append(record)
            records_since_last_log += 1

        # Periodic save
        should_save = (
            len(snapshot_buffer) > 0 and 
            (now_ts - last_save_time > SAVE_INTERVAL or len(snapshot_buffer) >= MAX_BUFFER_SIZE)
        )
        
        if should_save:
            save_buffer_to_disk()

        # Status log every 60 seconds
        if now_ts - last_log_time >= 60:
            total = 0
            if FILENAME.exists():
                try:
                    df = pd.read_parquet(FILENAME)
                    total = len(df)
                except:
                    pass
            
            # Show current prices in status
            with data_lock:
                up_b = prices[TOKEN_ID_UP]["best_bid"]
                up_a = prices[TOKEN_ID_UP]["best_ask"]
            
            print(f"[STATUS] +{records_since_last_log} records | Total: {total} | UP bid/ask: {up_b}/{up_a}")
            records_since_last_log = 0
            last_log_time = now_ts

        time.sleep(SAMPLE_INTERVAL)

# ============ EXECUTION ============
if __name__ == "__main__":
    print("=" * 50)
    print("Polymarket Orderbook Capturer")
    print("=" * 50)
    print(f"Market: {MARKET_SLUG}")
    print(f"Output: {FILENAME}")
    print("Press Ctrl+C to stop and savçe")
    print("=" * 50)
    
    # Start REST API price poller
    t_poller = threading.Thread(target=price_poller, daemon=True)
    t_poller.start()
    
    # Start WebSocket for last_trade_price events
    t_ws = threading.Thread(target=run_websocket, daemon=True)
    t_ws.start()
    
    try:
        sampler_and_saver()
    except KeyboardInterrupt:
        graceful_shutdown()
    except Exception as e:
        print(f"[ERROR] {e}")
        graceful_shutdown()
