import asyncio
import websockets
import json
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

# 1. Configuration
WS_URL = "wss://ws-live-data.polymarket.com"
DATA_DIR = Path("DataCollection/BitcoinPrices")
DATA_DIR.mkdir(exist_ok=True)
FILENAME = DATA_DIR / "1766692800-prices.parquet"

# Subscription (Chainlink Feed)
# subscribe_message = {
#   "action": "subscribe",
#   "subscriptions": [{
#     "topic": "crypto_prices_chainlink",
#     "type": "*",
#     "filters": "{\"symbol\":\"btc/usd\"}"
#   }]
# }
subscribe_message = {
  "action": "subscribe",
  "subscriptions": [{
    "topic": "crypto_prices",
    "type": "update",
    "filters": "btcusdt,ethusdt"
  }]
}

# Watchlist
MY_WATCHLIST = ["btcusdt"]

async def run_bot():
    # --- BUFFER SETUP ---
    price_buffer = []
    last_save_time = time.time()
    
    async with websockets.connect(WS_URL) as websocket:
        print(f"Connected to {WS_URL}")
        
        # Send Subscription
        await websocket.send(json.dumps(subscribe_message))
        print("Subscribed to global feed. Waiting for data...")

        # Heartbeat Task
        async def keep_alive():
            while True:
                await asyncio.sleep(5)
                try:
                    await websocket.ping()
                except:
                    break
        asyncio.create_task(keep_alive())

        # Main Loop
        try:
            async for message in websocket:
                if not message: continue
                
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                if "error" in data:
                    print(f"❌ SERVER ERROR: {data['error']}")
                    continue

                # Process Price Updates
                if data.get("topic") == "crypto_prices_chainlink" and data.get("type") == "update":
                    payload = data.get("payload", {})
                    symbol = payload.get("symbol")
                    price = payload.get("value")
                    ts = payload.get("timestamp", time.time() * 1000) # Server timestamp
                    
                    if symbol in MY_WATCHLIST:
                        # 1. Print to Console
                        print(f"✅ {symbol.upper()}: ${price}")
                        
                        # 2. Add to Buffer (RAM)
                        record = {
                            "system_timestamp": datetime.now(),
                            "server_timestamp": ts, # Keep original server time
                            "symbol": symbol,
                            "price": float(price)
                        }
                        price_buffer.append(record)

                # --- SAVING LOGIC ---
                # Save if: 60 seconds passed OR buffer has > 100 items
                current_time = time.time()
                if price_buffer and (current_time - last_save_time > 60 or len(price_buffer) >= 100):
                    print(f"💾 Flushing {len(price_buffer)} prices to {FILENAME}...")
                    
                    # Convert to DataFrame
                    new_df = pd.DataFrame(price_buffer)
                    
                    # Append to existing file logic
                    if FILENAME.exists():
                        try:
                            existing_df = pd.read_parquet(FILENAME)
                            final_df = pd.concat([existing_df, new_df], ignore_index=True)
                        except Exception as e:
                            print(f"⚠️ Error reading existing file: {e}. Creating new one.")
                            final_df = new_df
                    else:
                        final_df = new_df
                    
                    # Write to disk
                    final_df.to_parquet(FILENAME, index=False)
                    
                    # Clear buffer & reset timer
                    price_buffer = []
                    last_save_time = current_time
                    print("✅ Save complete.")

        except websockets.ConnectionClosed:
            print("Connection closed. Reconnecting...")
            
        except KeyboardInterrupt:
            # This is handled by the outer block, but good to have safety here
            pass

    # --- FINAL SAVE ON EXIT ---
    # This runs when the connection closes or loop ends
    if price_buffer:
        print(f"🛑 Stopping... Saving final {len(price_buffer)} records.")
        new_df = pd.DataFrame(price_buffer)
        if FILENAME.exists():
            existing_df = pd.read_parquet(FILENAME)
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        final_df.to_parquet(FILENAME, index=False)

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Bot stopped by user.")