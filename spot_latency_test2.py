#!/usr/bin/env python3
"""
Definitive "who updates fastest" BTC spot feed shootout (Binance / Coinbase / Kraken / Bitfinex)

What this script is designed to answer:
- When the consolidated BTC spot price moves by >= MOVE_USD (e.g., $4),
  which websocket feed is the FIRST to show that move (based on your local receipt time)?

Why this is much closer to "definitive" than your previous script:
- Uses perf_counter_ns() (monotonic, nanosecond, no clock-skew nonsense)
- Avoids slow timestamp parsing in the hot path (no pandas)
- Compares feeds on the SAME machine, SAME event loop, SAME write path
- Defines "price jump events" from a consolidated price (median across latest venue prices)
  so USD vs USDT basis doesn’t break your logic
- Treats batching correctly: one “update” per WS message (not per trade inside the message)

Outputs:
- events.csv: one row per detected move event (winner, leads, who crossed when)
- Console summary: win counts + median lead vs runner-up

Run:
  python3 fastest_feed_btc.py --seconds 900 --move-usd 4 --window-ms 800

Tip for best-quality measurement:
- Run this on a quiet VPS close to exchange infra (e.g., AWS us-east-1), pin CPU if you can,
  and run multiple sessions. The script prints stable stats quickly, but repetition helps.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

import websockets


# ----------------------------
# Config: exchanges + products
# ----------------------------

PAIRS = {
    "binance": "btcusdt",   # trades stream
    "kraken": "XBT/USD",    # trade subscription
    "bitfinex": "tBTCUSD",  # trades channel symbol
    "coinbase": "BTC-USD",  # market_trades channel
}


# ----------------------------
# Utility: monotonic time
# ----------------------------

def now_ns() -> int:
    # Monotonic, high resolution, immune to NTP jumps.
    return time.perf_counter_ns()


def ns_to_ms(ns: int) -> float:
    return ns / 1_000_000.0


def median_price(latest: Dict[str, float]) -> Optional[float]:
    vals = [p for p in latest.values() if p is not None and math.isfinite(p)]
    if len(vals) < 2:
        return None
    return statistics.median(vals)


# ----------------------------
# Data structures
# ----------------------------

@dataclass(slots=True)
class Tick:
    exchange: str
    price: float
    recv_ns: int


@dataclass(slots=True)
class ExchangeState:
    last_price: Optional[float] = None
    last_recv_ns: int = 0


@dataclass(slots=True)
class MoveEvent:
    event_id: int
    direction: str  # "UP" or "DOWN"
    anchor_median: float
    target_price: float  # anchor_median +/- MOVE_USD
    opened_ns: int

    # When each exchange FIRST crosses target (according to our consolidated event definition)
    crossed_ns: Dict[str, int] = field(default_factory=dict)
    crossed_price: Dict[str, float] = field(default_factory=dict)

    # Debug: who triggered event open (not necessarily winner)
    trigger_exchange: str = ""
    trigger_recv_ns: int = 0


# ----------------------------
# Connection wrapper
# ----------------------------

async def connect_forever(uri: str, name: str, *, ping_interval: int = 20):
    """
    Reconnect loop that yields an open websocket.
    """
    backoff = 0.25
    while True:
        try:
            async with websockets.connect(
                uri,
                ping_interval=ping_interval,
                ping_timeout=ping_interval,
                max_queue=1024,
                compression=None,
            ) as ws:
                print(f"[{name}] connected")
                backoff = 0.25
                yield ws
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[{name}] connection failed: {e} (retry in {backoff:.2f}s)")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 5.0)


# ----------------------------
# Exchange adapters (one tick per WS message)
# ----------------------------

async def feed_binance(q: asyncio.Queue[Tick]):
    uri = f"wss://stream.binance.com:9443/ws/{PAIRS['binance']}@trade"
    async for ws in connect_forever(uri, "binance"):
        try:
            while True:
                msg = await ws.recv()
                recv = now_ns()
                data = json.loads(msg)
                # { p: "price", T: trade time ms, ... }
                price = float(data["p"])
                await q.put(Tick("binance", price, recv))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[binance] error: {e}")


async def feed_kraken(q: asyncio.Queue[Tick]):
    uri = "wss://ws.kraken.com"
    async for ws in connect_forever(uri, "kraken"):
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": [PAIRS["kraken"]],
            "subscription": {"name": "trade"}
        }))
        try:
            while True:
                msg = await ws.recv()
                recv = now_ns()
                data = json.loads(msg)

                # Trade payload is a list:
                # [channelID, [[price, vol, time, side, orderType, misc], ...], channelName, pair]
                if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list) and data[1]:
                    # One "update" per message: use LAST trade in this message
                    t = data[1][-1]
                    price = float(t[0])
                    await q.put(Tick("kraken", price, recv))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[kraken] error: {e}")


async def feed_bitfinex(q: asyncio.Queue[Tick]):
    uri = "wss://api-pub.bitfinex.com/ws/2"
    async for ws in connect_forever(uri, "bitfinex"):
        await ws.send(json.dumps({
            "event": "subscribe",
            "channel": "trades",
            "symbol": PAIRS["bitfinex"],
        }))
        try:
            while True:
                msg = await ws.recv()
                recv = now_ns()
                data = json.loads(msg)
                # Trade execution update: [chanId, "tu", [ID, MTS, AMOUNT, PRICE]]
                if isinstance(data, list) and len(data) > 2 and data[1] == "tu":
                    trade = data[2]
                    price = float(trade[3])
                    await q.put(Tick("bitfinex", price, recv))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[bitfinex] error: {e}")


async def feed_coinbase(q: asyncio.Queue[Tick]):
    uri = "wss://advanced-trade-ws.coinbase.com"
    async for ws in connect_forever(uri, "coinbase"):
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": [PAIRS["coinbase"]],
        }))
        try:
            while True:
                msg = await ws.recv()
                recv = now_ns()
                data = json.loads(msg)

                if data.get("channel") != "market_trades":
                    continue

                # One "update" per message: take the LAST trade in the message
                # (Batching means you did NOT see earlier trades earlier; treating each trade
                # as separate would artificially “speed up” Coinbase.)
                last_trade_price = None
                events = data.get("events") or []
                for ev in events:
                    trades = ev.get("trades") or []
                    if trades:
                        last_trade_price = trades[-1].get("price")

                if last_trade_price is not None:
                    price = float(last_trade_price)
                    await q.put(Tick("coinbase", price, recv))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[coinbase] error: {e}")


# ----------------------------
# Move-event detector
# ----------------------------

@dataclass(slots=True)
class Stats:
    wins: Dict[str, int] = field(default_factory=dict)
    leads_ms: Dict[str, List[float]] = field(default_factory=dict)  # winner lead vs runner-up
    total_events: int = 0
    completed_events: int = 0
    dropped_events: int = 0


def ensure_stats_keys(stats: Stats, exchanges: List[str]):
    for ex in exchanges:
        stats.wins.setdefault(ex, 0)
        stats.leads_ms.setdefault(ex, [])


async def detector_task(
    q: asyncio.Queue[Tick],
    *,
    move_usd: float,
    window_ms: int,
    cooldown_ms: int,
    min_feeds: int,
    csv_path: str,
    run_seconds: int,
):
    """
    Watches incoming ticks, maintains latest prices, and defines "MOVE events" by the median price.

    Event definition:
      - Let M be the running median of latest venue prices (requires >=2 venues live).
      - When |M - anchor| >= move_usd and no active event, we open an event with:
            direction = sign(M - anchor)
            target    = anchor + direction*move_usd
      - We then record, for each exchange, the first time its price crosses target in that direction.
      - After window_ms elapses, or after all exchanges have crossed, we finalize the event if at least
        min_feeds crossed; winner = earliest cross time.
      - We then set anchor = current median and enforce cooldown_ms before opening another event.
    """
    exchanges = list(PAIRS.keys())
    state: Dict[str, ExchangeState] = {ex: ExchangeState() for ex in exchanges}
    latest_prices: Dict[str, Optional[float]] = {ex: None for ex in exchanges}

    stats = Stats()
    ensure_stats_keys(stats, exchanges)

    # Event state
    anchor_median: Optional[float] = None
    active: Optional[MoveEvent] = None
    next_open_allowed_ns = 0
    event_id = 0

    # CSV output
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    f = open(csv_path, "w", newline="")
    w = csv.writer(f)
    w.writerow([
        "event_id",
        "opened_ms",
        "direction",
        "anchor_median",
        "target_price",
        "trigger_exchange",
        "winner_exchange",
        "winner_ms",
        "runner_up_exchange",
        "runner_up_ms",
        "winner_lead_ms",
        "crossed_exchanges",
    ])
    f.flush()

    start_ns = now_ns()
    end_ns = start_ns + int(run_seconds * 1e9)

    def maybe_finalize(now_ns_: int):
        nonlocal active, anchor_median, next_open_allowed_ns, stats

        if active is None:
            return

        age_ms = ns_to_ms(now_ns_ - active.opened_ns)
        if age_ms < window_ms:
            # If all exchanges crossed, we can close early.
            if len(active.crossed_ns) < len(exchanges):
                return

        stats.total_events += 1

        # Require at least min_feeds crossings to declare a meaningful "race"
        if len(active.crossed_ns) < min_feeds:
            stats.dropped_events += 1
            # Still update anchor to avoid getting stuck on noisy partials
            cur_med = median_price({k: v for k, v in latest_prices.items() if v is not None})
            if cur_med is not None:
                anchor_median = cur_med
            active = None
            next_open_allowed_ns = now_ns_ + int(cooldown_ms * 1e6)
            return

        # Determine winner and runner-up by earliest cross time
        items = sorted(active.crossed_ns.items(), key=lambda kv: kv[1])
        (winner, winner_t) = items[0]
        runner_up, runner_t = ("", 0)
        if len(items) >= 2:
            runner_up, runner_t = items[1]
        else:
            runner_up, runner_t = ("(none)", winner_t)

        winner_ms = ns_to_ms(winner_t - start_ns)
        runner_ms = ns_to_ms(runner_t - start_ns)
        lead_ms = ns_to_ms(runner_t - winner_t) if runner_t >= winner_t else 0.0

        stats.completed_events += 1
        stats.wins[winner] += 1
        stats.leads_ms[winner].append(lead_ms)

        crossed_list = []
        for ex in exchanges:
            if ex in active.crossed_ns:
                crossed_list.append(f"{ex}:{ns_to_ms(active.crossed_ns[ex]-start_ns):.3f}")
        crossed_str = " ".join(crossed_list)

        w.writerow([
            active.event_id,
            ns_to_ms(active.opened_ns - start_ns),
            active.direction,
            f"{active.anchor_median:.2f}",
            f"{active.target_price:.2f}",
            active.trigger_exchange,
            winner,
            f"{winner_ms:.3f}",
            runner_up,
            f"{runner_ms:.3f}",
            f"{lead_ms:.3f}",
            crossed_str,
        ])
        f.flush()

        # Reset anchor & cooldown
        cur_med = median_price({k: v for k, v in latest_prices.items() if v is not None})
        if cur_med is not None:
            anchor_median = cur_med

        active = None
        next_open_allowed_ns = now_ns_ + int(cooldown_ms * 1e6)

    # Main loop
    last_print_ns = start_ns

    while now_ns() < end_ns:
        try:
            tick: Tick = await asyncio.wait_for(q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            # Periodically try to finalize if active event expired
            maybe_finalize(now_ns())
            continue

        # Update per-exchange state
        st = state[tick.exchange]
        st.last_price = tick.price
        st.last_recv_ns = tick.recv_ns
        latest_prices[tick.exchange] = tick.price

        # Need at least 2 venues to compute median anchor
        med = median_price({k: v for k, v in latest_prices.items() if v is not None})
        if med is None:
            continue

        if anchor_median is None:
            anchor_median = med
            continue

        # If event active: record crossings
        if active is not None:
            # Check if this exchange crosses the event target in the event direction
            if tick.exchange not in active.crossed_ns:
                if active.direction == "UP" and tick.price >= active.target_price:
                    active.crossed_ns[tick.exchange] = tick.recv_ns
                    active.crossed_price[tick.exchange] = tick.price
                elif active.direction == "DOWN" and tick.price <= active.target_price:
                    active.crossed_ns[tick.exchange] = tick.recv_ns
                    active.crossed_price[tick.exchange] = tick.price

            # finalize if window elapsed or all crossed
            maybe_finalize(tick.recv_ns)

        # If no event active: maybe open one
        else:
            if tick.recv_ns < next_open_allowed_ns:
                continue

            diff = med - anchor_median
            if abs(diff) >= move_usd:
                direction = "UP" if diff > 0 else "DOWN"
                target = anchor_median + (move_usd if diff > 0 else -move_usd)

                active = MoveEvent(
                    event_id=event_id,
                    direction=direction,
                    anchor_median=anchor_median,
                    target_price=target,
                    opened_ns=tick.recv_ns,
                    trigger_exchange=tick.exchange,
                    trigger_recv_ns=tick.recv_ns,
                )
                event_id += 1

                # Important: some exchanges may have already crossed BEFORE we opened the event
                # (print earlier tick but median threshold wasn't met until now). If so, they should win.
                for ex in exchanges:
                    ex_price = latest_prices.get(ex)
                    if ex_price is None:
                        continue
                    ex_state = state[ex]
                    if direction == "UP" and ex_price >= target:
                        active.crossed_ns.setdefault(ex, ex_state.last_recv_ns)
                        active.crossed_price.setdefault(ex, ex_price)
                    elif direction == "DOWN" and ex_price <= target:
                        active.crossed_ns.setdefault(ex, ex_state.last_recv_ns)
                        active.crossed_price.setdefault(ex, ex_price)

        # periodic progress prints
        if tick.recv_ns - last_print_ns > int(2e9):
            last_print_ns = tick.recv_ns
            live = {k: v for k, v in latest_prices.items() if v is not None}
            med_now = median_price(live) if live else None
            print(f"[live] median={med_now:.2f}  prices={ {k: round(v,2) for k,v in live.items()} }"
                  f"  events_completed={stats.completed_events} dropped={stats.dropped_events}")

    # Finalize any lingering event
    maybe_finalize(now_ns())
    f.close()

    # Print summary
    print("\n==================== SUMMARY ====================")
    print(f"Ran for {run_seconds}s")
    print(f"Events completed: {stats.completed_events}  dropped (too few crossings): {stats.dropped_events}")
    print("")
    rows = []
    for ex in exchanges:
        wins = stats.wins.get(ex, 0)
        leads = stats.leads_ms.get(ex, [])
        med_lead = statistics.median(leads) if leads else float("nan")
        p90_lead = statistics.quantiles(leads, n=10)[8] if len(leads) >= 10 else float("nan")
        rows.append((wins, ex, med_lead, p90_lead, len(leads)))
    rows.sort(reverse=True)

    for wins, ex, med_lead, p90_lead, n in rows:
        print(f"{ex:8s} wins={wins:4d}  lead_median_ms={med_lead:8.3f}  lead_p90_ms={p90_lead:8.3f}  n={n}")

    print(f"\nWrote event log to: {csv_path}")
    print("=================================================\n")


# ----------------------------
# Optional: event loop lag monitor
# ----------------------------

async def loop_lag_monitor(*, interval_ms: int = 10):
    """
    Detects whether your process is falling behind (CPU pressure / parsing cost).
    If lag is huge, your “fastest feed” result is contaminated by your own consumer.
    """
    interval_s = interval_ms / 1000.0
    expected = time.perf_counter()
    while True:
        await asyncio.sleep(interval_s)
        now = time.perf_counter()
        expected += interval_s
        lag_ms = (now - expected) * 1000.0
        if lag_ms > 5.0:
            print(f"[loop-lag] {lag_ms:.2f} ms (your process is behind)")


# ----------------------------
# Main
# ----------------------------

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=900, help="How long to run (default 900s)")
    p.add_argument("--move-usd", type=float, default=4.0, help="Move threshold in USD (default $4)")
    p.add_argument("--window-ms", type=int, default=800, help="How long to wait for others to cross (default 800ms)")
    p.add_argument("--cooldown-ms", type=int, default=400, help="Debounce between events (default 400ms)")
    p.add_argument("--min-feeds", type=int, default=2, help="Min crossings required to score an event (default 2)")
    p.add_argument("--out", type=str, default="events.csv", help="Output CSV path")
    p.add_argument("--monitor-lag", action="store_true", help="Print warnings if event loop is behind")
    args = p.parse_args()

    q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=100_000)

    tasks = [
        asyncio.create_task(feed_binance(q)),
        asyncio.create_task(feed_kraken(q)),
        asyncio.create_task(feed_bitfinex(q)),
        asyncio.create_task(feed_coinbase(q)),
        asyncio.create_task(detector_task(
            q,
            move_usd=args.move_usd,
            window_ms=args.window_ms,
            cooldown_ms=args.cooldown_ms,
            min_feeds=args.min_feeds,
            csv_path=args.out,
            run_seconds=args.seconds,
        )),
    ]

    if args.monitor_lag:
        tasks.append(asyncio.create_task(loop_lag_monitor()))

    # Clean shutdown on Ctrl+C
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _handle_sig(*_):
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_sig)
        except NotImplementedError:
            pass  # Windows

    await stop.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    # Optional: uvloop if available (lower jitter)
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass