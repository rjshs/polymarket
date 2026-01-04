#!/usr/bin/env python3
"""
CoinbaseSpot.py

Streams Coinbase Advanced Trade WS level2 for BTC-USD, maintains top-of-book,
writes CSV rows:
timestamp_event,timestamp_recv,best_bid,best_ask,mid

Lightweight prints + targeted debug if no mid is produced.
"""

import argparse
import asyncio
import csv
import json
import os
import signal
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import websockets

WS_URL = "wss://advanced-trade-ws.coinbase.com"


def utc_now_iso(us: bool = True) -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds" if us else "seconds")


def parse_float(s: str) -> float:
    return float(s)


def update_book_side(book: Dict[float, float], price: float, qty: float) -> None:
    if qty == 0.0:
        book.pop(price, None)
    else:
        book[price] = qty


def recompute_best_bid(bids: Dict[float, float]) -> Optional[float]:
    return max(bids.keys()) if bids else None


def recompute_best_ask(asks: Dict[float, float]) -> Optional[float]:
    return min(asks.keys()) if asks else None


def extract_event_timestamp(event: dict, msg_timestamp: Optional[str]) -> str:
    updates = event.get("updates") or []
    if event.get("type") == "update" and updates:
        times = [u.get("event_time") for u in updates if u.get("event_time")]
        if times:
            return max(times)
    return msg_timestamp or ""


def normalize_side(raw: str) -> Optional[str]:
    """
    Normalize various side labels to 'bid' or 'ask'.
    Returns None if unrecognized.
    """
    s = (raw or "").strip().lower()
    if s in ("bid", "bids", "buy", "buyer", "b"):
        return "bid"
    if s in ("ask", "asks", "sell", "seller", "offer", "o", "a"):
        return "ask"
    return None


class MidWriter:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._file = open(csv_path, "a", newline="")
        self._writer = csv.writer(self._file)
        if self._file.tell() == 0:
            self._writer.writerow(["timestamp_event", "timestamp_recv", "best_bid", "best_ask", "mid"])
            self._file.flush()

    def write_row(self, timestamp_event: str, best_bid: float, best_ask: float, mid: float) -> None:
        self._writer.writerow(
            [timestamp_event, utc_now_iso(), f"{best_bid:.2f}", f"{best_ask:.2f}", f"{mid:.2f}"]
        )
        self._file.flush()

    def close(self):
        try:
            self._file.flush()
        finally:
            self._file.close()


async def stream_level2_to_csv(
    csv_path: str,
    product_id: str,
    print_every_rows: int,
    status_every_sec: int,
    debug_first_events: int,
    debug_if_no_mid_after_events: int,
    jwt: Optional[str] = None,
) -> None:
    bids: Dict[float, float] = {}
    asks: Dict[float, float] = {}
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None

    writer = MidWriter(csv_path)
    stop = asyncio.Event()

    def _handle_sig(*_args):
        stop.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    backoff = 1.0
    max_backoff = 30.0

    rows_written = 0
    msg_count = 0
    level2_event_count = 0

    last_mid: Optional[float] = None
    last_best_bid: Optional[float] = None
    last_best_ask: Optional[float] = None
    last_event_ts: str = ""

    side_counts = Counter()
    update_key_samples = Counter()  # tracks observed keysets (stringified)
    printed_debug_events = 0
    printed_no_mid_debug = False

    async def status_printer():
        while not stop.is_set():
            await asyncio.sleep(status_every_sec)
            if stop.is_set():
                break
            if last_mid is None:
                print(f"[status] {utc_now_iso(us=False)} msgs={msg_count} level2_events={level2_event_count} rows={rows_written} (no mid yet)")
            else:
                print(
                    f"[status] {utc_now_iso(us=False)} msgs={msg_count} level2_events={level2_event_count} rows={rows_written} "
                    f"mid={last_mid:.2f} bid={last_best_bid:.2f} ask={last_best_ask:.2f} ev_ts={last_event_ts}"
                )

    status_task = asyncio.create_task(status_printer())

    try:
        while not stop.is_set():
            try:
                print(f"[info] connecting to {WS_URL} ...")
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=None,
                    max_size=None,  # IMPORTANT: allow >1MB snapshots
                ) as ws:
                    # Subscribe heartbeats (optional but helps keep open)
                    hb = {"type": "subscribe", "channel": "heartbeats"}
                    if jwt:
                        hb["jwt"] = jwt
                    await ws.send(json.dumps(hb))

                    # Subscribe level2
                    sub = {"type": "subscribe", "channel": "level2", "product_ids": [product_id]}
                    if jwt:
                        sub["jwt"] = jwt
                    await ws.send(json.dumps(sub))

                    print(f"[info] subscribed: heartbeats + level2 ({product_id})")
                    backoff = 1.0

                    while not stop.is_set():
                        raw = await ws.recv()
                        msg_count += 1

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        msg_ts = msg.get("timestamp")
                        events = msg.get("events")
                        if not events:
                            continue

                        for event in events:
                            # Heartbeats events don’t have product_id/updates; skip early
                            if event.get("product_id") != product_id:
                                continue

                            updates = event.get("updates") or []
                            if not isinstance(updates, list) or len(updates) == 0:
                                continue

                            level2_event_count += 1

                            # Debug: print first few level2 events (compact)
                            if printed_debug_events < debug_first_events:
                                printed_debug_events += 1
                                first_u = updates[0] if updates else {}
                                keys = sorted(list(first_u.keys()))
                                sides = [str(u.get("side")) for u in updates[:2]]
                                print(
                                    f"[debug] level2 event #{printed_debug_events} type={event.get('type')} "
                                    f"updates={len(updates)} msg_ts={msg_ts} "
                                    f"first_update_keys={keys} first_sides={sides}"
                                )

                            for u in updates:
                                # record keyset sample for diagnostics
                                if u and isinstance(u, dict):
                                    update_key_samples[str(tuple(sorted(u.keys())))] += 1

                                raw_side = u.get("side")
                                side_counts[str(raw_side)] += 1

                                side = normalize_side(str(raw_side))
                                if side is None:
                                    continue

                                price_s = u.get("price_level")
                                qty_s = u.get("new_quantity")

                                # If schema differs, capture it via debug_if_no_mid block below
                                if price_s is None or qty_s is None:
                                    continue

                                try:
                                    price = parse_float(price_s)
                                    qty = parse_float(qty_s)
                                except (TypeError, ValueError):
                                    continue

                                if side == "bid":
                                    update_book_side(bids, price, qty)
                                    if best_bid is None:
                                        best_bid = price if qty > 0 else recompute_best_bid(bids)
                                    else:
                                        if qty > 0 and price > best_bid:
                                            best_bid = price
                                        elif qty == 0 and price == best_bid:
                                            best_bid = recompute_best_bid(bids)

                                elif side == "ask":
                                    update_book_side(asks, price, qty)
                                    if best_ask is None:
                                        best_ask = price if qty > 0 else recompute_best_ask(asks)
                                    else:
                                        if qty > 0 and price < best_ask:
                                            best_ask = price
                                        elif qty == 0 and price == best_ask:
                                            best_ask = recompute_best_ask(asks)

                            # If still no mid after many events, print one-time diagnostic
                            if (best_bid is None or best_ask is None) and (level2_event_count >= debug_if_no_mid_after_events) and not printed_no_mid_debug:
                                printed_no_mid_debug = True
                                top_sides = side_counts.most_common(8)
                                top_keysets = update_key_samples.most_common(3)
                                print("[debug] STILL NO MID after many level2 events.")
                                print(f"[debug] best_bid={best_bid} best_ask={best_ask} bids_levels={len(bids)} asks_levels={len(asks)}")
                                print(f"[debug] top raw 'side' values observed: {top_sides}")
                                print(f"[debug] top update keysets observed: {top_keysets}")
                                print("[debug] If sides look weird or keys don’t include price_level/new_quantity, paste this block back to me.")

                            if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
                                mid = 0.5 * (best_bid + best_ask)
                                timestamp_event = extract_event_timestamp(event, msg_ts)

                                writer.write_row(timestamp_event, best_bid, best_ask, mid)
                                rows_written += 1

                                last_mid = mid
                                last_best_bid = best_bid
                                last_best_ask = best_ask
                                last_event_ts = timestamp_event

                                if rows_written % print_every_rows == 0:
                                    print(
                                        f"[info] wrote {rows_written} rows | mid={mid:.2f} bid={best_bid:.2f} ask={best_ask:.2f} last_ev_ts={timestamp_event}"
                                    )

            except (websockets.ConnectionClosed, OSError) as e:
                if stop.is_set():
                    break
                print(f"[warn] disconnected ({type(e).__name__}): {e}. Reconnecting in {backoff:.1f}s...", file=sys.stderr)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

    finally:
        stop.set()
        status_task.cancel()
        writer.close()
        print(f"[info] stopped. total rows written: {rows_written}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=str, default="coinbase_spot_btc",
                    help="Output directory (relative to CWD). Default: coinbase_spot_btc")
    ap.add_argument("--out", type=str, default=None, help="Output CSV filename (optional).")
    ap.add_argument("--product", type=str, default="BTC-USD", help="Product id (default: BTC-USD)")
    ap.add_argument("--print-every-rows", type=int, default=200, help="Print a line every N rows written.")
    ap.add_argument("--status-every-sec", type=int, default=30, help="Print status every N seconds.")
    ap.add_argument("--debug-first-events", type=int, default=3, help="Print compact debug for first N level2 events.")
    ap.add_argument("--debug-if-no-mid-after-events", type=int, default=50,
                    help="If no mid after N level2 events, print one diagnostic block.")
    ap.add_argument("--jwt-env", type=str, default="COINBASE_WS_JWT",
                    help="Env var name for optional JWT (default: COINBASE_WS_JWT). Leave unset for anon.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.out:
        out_path = out_dir / args.out
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"coinbase_{args.product.replace('-', '').lower()}_mid_{ts}_utc.csv"

    jwt = os.environ.get(args.jwt_env)  # optional

    print(f"[info] writing to: {out_path}")
    print(f"[info] product: {args.product}")
    print(f"[info] auth: {'jwt' if jwt else 'anonymous'}")
    print("[info] press Ctrl+C to stop")

    asyncio.run(
        stream_level2_to_csv(
            csv_path=str(out_path),
            product_id=args.product,
            print_every_rows=args.print_every_rows,
            status_every_sec=args.status_every_sec,
            debug_first_events=args.debug_first_events,
            debug_if_no_mid_after_events=args.debug_if_no_mid_after_events,
            jwt=jwt,
        )
    )


if __name__ == "__main__":
    main()
