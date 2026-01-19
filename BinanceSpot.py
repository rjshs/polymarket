from __future__ import annotations

import asyncio
import json
import logging
import time
import os
from datetime import datetime
from typing import Optional, Set, List, Dict
import pandas as pd
import websockets
from pmm.core.protocols import PriceFeedPort
from pmm.core.logging import get_logger

logger = get_logger(__name__)

# Constants
WS_URL = "wss://ws-live-data.polymarket.com"
MAX_QUEUE_SIZE = 4

# Parquet Logging Constants
PARQUET_FILE = "chainlink_trades.parquet"
BUFFER_SIZE = 100

class ChainlinkWsFeed:
    """
    Polymarket/Chainlink WebSocket adapter for reference prices.
    Polls wss://ws-live-data.polymarket.com for "crypto_prices_chainlink".
    Implements PriceFeedPort and logs to Parquet.
    """

    def __init__(
        self, 
        symbol: str = "btc/usd", 
        ws_url: str = WS_URL
    ):
        self.symbol = symbol.lower()
        self.ws_url = ws_url
        
        # Internal state
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._latest_price: Optional[tuple[float, float]] = None # (monotonic_ts, price)
        
        # Subscribers
        self._subscribers: Set[asyncio.Queue[tuple[float, float]]] = set()

        # Parquet Buffer
        self._parquet_buffer: List[Dict] = []

    async def start(self) -> None:
        """Start the background feed task."""
        if self._task is not None and not self._task.done():
            return
        
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"ChainlinkWsFeed started for {self.symbol}")

    async def stop(self) -> None:
        """Stop the background feed task."""
        self._stop_event.set()
        
        # Flush remaining buffer on stop
        if self._parquet_buffer:
            self._flush_buffer()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("ChainlinkWsFeed stopped")

    def get_latest(self) -> tuple[float, float] | None:
        """Non-blocking access to latest price."""
        if self._latest_price:
            ts, _ = self._latest_price
            if time.monotonic() - ts > 30.0:
                 return None
        return self._latest_price

    def subscribe(self) -> asyncio.Queue[tuple[float, float]]:
        """Return a bounded queue for updates."""
        q: asyncio.Queue[tuple[float, float]] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[tuple[float, float]]) -> None:
        self._subscribers.discard(q)

    async def _run_loop(self) -> None:
        backoff = 1.0
        max_backoff = 30.0
        
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url) as ws:
                    logger.event("feed.connect", feed="chainlink", symbol=self.symbol)
                    
                    subscribe_msg = {
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*"
                        }]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Chainlink subscribed to crypto_prices_chainlink")
                    backoff = 1.0

                    hb_task = asyncio.create_task(self._keep_alive(ws))
                    
                    try:
                        async for message in ws:
                            if self._stop_event.is_set():
                                break
                            await self._handle_message(message)
                    finally:
                        hb_task.cancel()
                        
            except (websockets.ConnectionClosed, OSError, asyncio.CancelledError) as e:
                if self._stop_event.is_set():
                    break
                if isinstance(e, asyncio.CancelledError):
                    raise
                
                logger.event("feed.disconnect", feed="chainlink", error=str(e), next_retry_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2.0)
            except Exception as e:
                logger.error(f"Unexpected error in Chainlink loop: {e}", exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2.0)

    async def _keep_alive(self, ws: websockets.WebSocketClientProtocol) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(5)
            try:
                await ws.ping()
            except:
                break

    async def _handle_message(self, raw: str | bytes) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if "error" in data:
            logger.warning(f"[Chainlink] Feed Error: {data['error']}")
            return

        if data.get("topic") == "crypto_prices_chainlink" and data.get("type") == "update":
            payload = data.get("payload", {})
            symbol = payload.get("symbol")
            price_val = payload.get("value")
            block_ts = payload.get("block_timestamp") # Chainlink update time

            # Filter symbol
            if (symbol or "").lower() != self.symbol:
                return

            ts_mono = time.monotonic()

            try:
                price_float = float(price_val)
            except (ValueError, TypeError):
                return

            # --- Update Internal State ---
            self._update_latest(ts_mono, price_float)

            # --- Log to Parquet Buffer ---
            # We try to use the block timestamp from payload if available for accuracy,
            # otherwise fall back to current UTC time.
            if block_ts:
                # Assuming block_timestamp is in seconds or ms, adjust if needed.
                # Usually Chainlink feeds provide unix timestamp.
                try:
                    dt = datetime.utcfromtimestamp(int(block_ts))
                except:
                    dt = datetime.utcnow()
            else:
                dt = datetime.utcnow()

            record = {
                'timestamp': dt,
                'exchange': 'chainlink_polymarket',
                'symbol': self.symbol,
                'price': price_float,
                'size': 0.0, # Chainlink updates don't have size/volume
                'side': '',   # Oracle updates don't have a side
                'trade_id': ''
            }
            
            self._parquet_buffer.append(record)
            
            # Flush if buffer full
            if len(self._parquet_buffer) >= BUFFER_SIZE:
                self._flush_buffer()

    def _flush_buffer(self) -> None:
        """Write buffer to parquet file."""
        if not self._parquet_buffer:
            return
            
        try:
            df = pd.DataFrame(self._parquet_buffer)
            
            if not os.path.exists(PARQUET_FILE):
                df.to_parquet(PARQUET_FILE, engine='fastparquet')
            else:
                df.to_parquet(PARQUET_FILE, engine='fastparquet', append=True)
            
            logger.info(f"[Chainlink] Flushed {len(self._parquet_buffer)} records to {PARQUET_FILE}")
            self._parquet_buffer = []
            
        except Exception as e:
            logger.error(f"[Chainlink] Failed to flush parquet: {e}")

    def _update_latest(self, ts: float, price: float) -> None:
        self._latest_price = (ts, price)
        # Log price update for existing log system (spot_prices.log)
        logger.event("price.update", feed="chainlink", price_usd=price, ts=ts)

        for q in list(self._subscribers):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass 
            try:
                q.put_nowait((ts, price))
            except asyncio.QueueFull:
                pass