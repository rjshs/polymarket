import asyncio
import websockets
import json
import pandas as pd
import os
import time
from datetime import datetime

# --- CONFIGURATION ---
PAIRS = {
    "binance": "btcusdt",
    "kraken": "XBT/USD",
    "bitfinex": "tBTCUSD",
    "coinbase": "BTC-USD"
}
OUTPUT_FILE = "unified_latency_data.parquet"
BUFFER_SIZE = 200

# Shared Queue for all feeds
msg_queue = asyncio.Queue()

# --- EXCHANGE ADAPTERS ---

async def connect_binance():
    uri = f"wss://stream.binance.com:9443/ws/{PAIRS['binance']}@trade"
    async for websocket in _connect_wrapper(uri, "binance"):
        try:
            while True:
                msg = await websocket.recv()
                ts_receipt = time.time() # Capture time immediately
                data = json.loads(msg)
                
                await msg_queue.put({
                    'receipt_ts': ts_receipt,
                    'exchange': 'binance',
                    'price': float(data['p']),
                    'exchange_ts': data['T'] / 1000.0, # Binance gives ms
                    'event_type': 'trade'
                })
        except Exception as e:
            print(f"Binance Error: {e}")

async def connect_kraken():
    uri = "wss://ws.kraken.com"
    async for websocket in _connect_wrapper(uri, "kraken"):
        await websocket.send(json.dumps({
            "event": "subscribe",
            "pair": [PAIRS['kraken']],
            "subscription": {"name": "trade"}
        }))
        
        try:
            while True:
                msg = await websocket.recv()
                ts_receipt = time.time()
                data = json.loads(msg)
                
                if isinstance(data, list): # Trade data is a list
                    # [channelID, [[price, vol, time, side, type, misc]], channelName, pair]
                    for t in data[1]:
                        await msg_queue.put({
                            'receipt_ts': ts_receipt,
                            'exchange': 'kraken',
                            'price': float(t[0]),
                            'exchange_ts': float(t[2]),
                            'event_type': 'trade'
                        })
        except Exception as e:
            print(f"Kraken Error: {e}")

async def connect_bitfinex():
    uri = "wss://api-pub.bitfinex.com/ws/2"
    async for websocket in _connect_wrapper(uri, "bitfinex"):
        await websocket.send(json.dumps({
            "event": "subscribe",
            "channel": "trades",
            "symbol": PAIRS['bitfinex']
        }))
        
        try:
            while True:
                msg = await websocket.recv()
                ts_receipt = time.time()
                data = json.loads(msg)
                
                # [ID, "tu", [ID, MTS, AMOUNT, PRICE]]
                if isinstance(data, list) and len(data) > 2 and data[1] == "tu":
                    trade = data[2]
                    await msg_queue.put({
                        'receipt_ts': ts_receipt,
                        'exchange': 'bitfinex',
                        'price': float(trade[3]),
                        'exchange_ts': trade[1] / 1000.0,
                        'event_type': 'trade'
                    })
        except Exception as e:
            print(f"Bitfinex Error: {e}")

async def connect_coinbase():
    uri = "wss://advanced-trade-ws.coinbase.com"
    async for websocket in _connect_wrapper(uri, "coinbase"):
        await websocket.send(json.dumps({
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": [PAIRS['coinbase']]
        }))
        
        try:
            while True:
                msg = await websocket.recv()
                ts_receipt = time.time()
                data = json.loads(msg)
                
                if data.get('channel') == 'market_trades':
                    for event in data.get('events', []):
                        for t in event.get('trades', []):
                            # Coinbase time is ISO string
                            # We parse it only if we need it, strictly purely for logging
                            # Parsing ISO strings is slow, do it in post-analysis if possible
                            # For now we just grab receipt time
                            await msg_queue.put({
                                'receipt_ts': ts_receipt,
                                'exchange': 'coinbase',
                                'price': float(t['price']),
                                'exchange_ts': pd.to_datetime(t['time']).timestamp(),
                                'event_type': 'trade'
                            })
        except Exception as e:
            print(f"Coinbase Error: {e}")

# --- UTILS ---

async def _connect_wrapper(uri, name):
    """Resilient connection generator"""
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print(f"Connected to {name}")
                yield ws
        except Exception as e:
            print(f"Connection failed {name}: {e}. Retrying...")
            await asyncio.sleep(1)

async def writer_task():
    """Single consumer to write parquet files"""
    buffer = []
    print("Writer task started...")
    
    while True:
        item = await msg_queue.get()
        buffer.append(item)
        
        if len(buffer) >= BUFFER_SIZE:
            df = pd.DataFrame(buffer)
            # Convert timestamp to proper datetime for Parquet
            df['receipt_dt'] = pd.to_datetime(df['receipt_ts'], unit='s')
            
            if not os.path.exists(OUTPUT_FILE):
                df.to_parquet(OUTPUT_FILE, engine='fastparquet')
            else:
                df.to_parquet(OUTPUT_FILE, engine='fastparquet', append=True)
            
            print(f"Flushed {len(buffer)} items. Latest: {buffer[-1]['exchange']} @ {buffer[-1]['price']}")
            buffer = []

async def main():
    # Delete old file
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    await asyncio.gather(
        connect_binance(),
        connect_kraken(),
        connect_bitfinex(),
        connect_coinbase(),
        writer_task()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping...")