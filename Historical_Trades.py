import requests
import pandas as pd
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List

# ============ CONFIGURATION ============
# Data storage directory
DATA_DIR = Path("DataCollection/BotTrades")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# DEFAULT SETTINGS - Edit these to run without arguments
DEFAULT_WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"  # REQUIRED: Wallet address to track
DEFAULT_MARKETS = None  # List of market slugs like ["bitcoin-up-or-down-december-21"] or None for all markets
DEFAULT_START = 1766692800  # Unix timestamp (seconds) for start time or None. Example: 1734775200
DEFAULT_END = 1766693700  # Unix timestamp (seconds) for end time or None. Example: 1734789600
DEFAULT_MAX_PAGES = None  # Maximum pages to fetch or None for unlimited
DEFAULT_SAVE = True  # Save trades to parquet file
OUTPUT_FILENAME = "1766692800-trades"  # Custom filename (without .parquet) or None for auto-generated

# API Configuration
BASE_URL = "https://data-api.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
# =======================================


def extract_market_slug(market_url):
    """
    Extract the market slug from a Polymarket URL
    Examples:
    - https://polymarket.com/event/bitcoin-up-or-down-december-21-3am-et -> bitcoin-up-or-down-december-21-3am-et
    - bitcoin-up-or-down-december-21-3am-et -> bitcoin-up-or-down-december-21-3am-et
    """
    if 'polymarket.com' in market_url:
        parts = market_url.rstrip('/').split('/')
        return parts[-1]
    else:
        return market_url.strip()


def get_condition_ids_from_slug(market_slug):
    """
    Get condition IDs for a market slug using Gamma API
    Returns list of condition IDs
    """
    print(f"Fetching condition IDs for market slug: {market_slug}")
    
    url = f"{GAMMA_URL}/events/{market_slug}"
    
    try:
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            event_data = response.json()
            condition_ids = []
            
            for market in event_data.get('markets', []):
                condition_id = market.get('conditionId')
                if condition_id:
                    condition_ids.append(condition_id)
                    print(f"  - Found condition ID: {condition_id} ({market.get('question')})")
            
            return condition_ids
        else:
            print(f"Error fetching market info: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error: {e}")
        return []


def fetch_activity_page(
    user: str,
    market: Optional[List[str]] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    limit: int = 500,
    offset: int = 0,
    activity_type: str = "TRADE",
    sort_direction: str = "DESC",
    politeness_delay_s: float = 0.15
):
    """
    Fetch a single page of activity from Polymarket Data API
    
    Args:
        user: Wallet address
        market: List of condition IDs to filter by
        start: Start unix timestamp (seconds)
        end: End unix timestamp (seconds)
        limit: Number of results per page
        offset: Pagination offset
        activity_type: Type of activity (TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION)
        sort_direction: ASC or DESC
        politeness_delay_s: Delay between requests
    """
    url = f"{BASE_URL}/activity"
    
    params = {
        'user': user,
        'limit': limit,
        'offset': offset,
        'type': activity_type,
        'sortDirection': sort_direction
    }
    
    # Add optional filters
    if market:
        params['market'] = ','.join(market)
    if start is not None:
        params['start'] = start
    if end is not None:
        params['end'] = end
    
    try:
        time.sleep(politeness_delay_s)
        response = requests.get(url, params=params, timeout=20)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limited, waiting 2 seconds...")
            time.sleep(2)
            return fetch_activity_page(user, market, start, end, limit, offset, 
                                      activity_type, sort_direction, politeness_delay_s)
        else:
            print(f"Error: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching activity: {e}")
        return []


def fetch_all_trades(
    user: str,
    market_slugs: Optional[List[str]] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    page_limit: int = 500,
    politeness_delay_s: float = 0.15,
    max_pages: Optional[int] = None
):
    """
    Fetch all trades for a user, optionally filtered by market slugs and time range
    
    Args:
        user: Wallet address
        market_slugs: List of market slugs to filter by
        start: Start unix timestamp (seconds)
        end: End unix timestamp (seconds)
        page_limit: Results per page
        politeness_delay_s: Delay between API calls
        max_pages: Maximum number of pages to fetch (None = unlimited)
    
    Returns:
        List of trade records
    """
    print(f"\nFetching trades for wallet: {user}")
    
    # Convert market slugs to condition IDs if provided
    condition_ids = []
    if market_slugs:
        print(f"\nResolving {len(market_slugs)} market slugs to condition IDs...")
        for slug in market_slugs:
            cids = get_condition_ids_from_slug(slug)
            condition_ids.extend(cids)
        
        if not condition_ids:
            print("Warning: No condition IDs found for provided market slugs")
            return []
        
        print(f"\nFiltering by {len(condition_ids)} condition IDs")
    
    # Time range info
    if start:
        print(f"Start time: {datetime.fromtimestamp(start)} ({start})")
    if end:
        print(f"End time: {datetime.fromtimestamp(end)} ({end})")
    
    all_trades = []
    seen_hashes = set()
    
    cur_end = end
    pages = 0
    last_oldest_t = None
    
    print("\nFetching trades...")
    
    while True:
        trades = fetch_activity_page(
            user=user,
            market=condition_ids if condition_ids else None,
            start=start,
            end=cur_end,
            limit=page_limit,
            offset=0,
            sort_direction="DESC",
            politeness_delay_s=politeness_delay_s
        )
        
        if not trades:
            print("No more trades found")
            break
        
        newest_t = int(trades[0]["timestamp"])
        oldest_t = int(trades[-1]["timestamp"])
        
        # Deduplicate trades
        added = 0
        for trade in trades:
            tx_hash = trade.get("transactionHash")
            if tx_hash and tx_hash not in seen_hashes:
                seen_hashes.add(tx_hash)
                all_trades.append(trade)
                added += 1
        
        pages += 1
        print(f"Page {pages}: Found {len(trades)} trades, added {added} unique | "
              f"Newest: {datetime.fromtimestamp(newest_t)} | "
              f"Oldest: {datetime.fromtimestamp(oldest_t)} | "
              f"Total unique: {len(all_trades)}")
        
        # Check for pagination progress
        if last_oldest_t is not None and oldest_t >= last_oldest_t:
            print("Warning: Pagination not progressing, stopping to avoid loop")
            break
        last_oldest_t = oldest_t
        
        # Check max pages limit
        if max_pages is not None and pages >= max_pages:
            print(f"Reached max pages limit ({max_pages})")
            break
        
        # Move end time back for next page
        cur_end = oldest_t - 1
        
        # Stop if we've gone past start time
        if start is not None and cur_end < start:
            print("Reached start time boundary")
            break
    
    # Sort by timestamp ascending (oldest first)
    all_trades.sort(key=lambda t: int(t["timestamp"]))
    
    print(f"\n{'='*80}")
    print(f"Total unique trades fetched: {len(all_trades)}")
    print(f"{'='*80}\n")
    
    return all_trades


def trades_to_dataframe(trades, market_slugs=None):
    """
    Convert trade records to a pandas DataFrame with selected fields
    """
    if not trades:
        return pd.DataFrame()
    
    records = []
    for trade in trades:
        record = {
            'timestamp': datetime.fromtimestamp(int(trade['timestamp'])),
            'unix_timestamp': int(trade['timestamp']),
            'slug': trade.get('slug', ''),
            'market_title': trade.get('title', ''),
            'side': trade.get('side', ''),
            'price': float(trade.get('price', 0)),
            'size': float(trade.get('size', 0)),
            'value': float(trade.get('value', 0)),
            'outcome': trade.get('outcome', ''),
            'condition_id': trade.get('conditionId', ''),
            'transaction_hash': trade.get('transactionHash', ''),
            'outcome_index': trade.get('outcomeIndex', ''),
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    return df


def save_trades_to_parquet(df, wallet_address, market_slugs=None, start=None, end=None, custom_filename=None):
    """
    Save trades DataFrame to Parquet file
    """
    if df.empty:
        print("No trades to save")
        return None
    
    # Use custom filename if provided
    if custom_filename:
        filename = f"{custom_filename}.parquet"
    else:
        # Create descriptive filename
        filename_parts = [f"trades_{wallet_address[:8]}"]
        
        if market_slugs:
            # Use first market slug (truncated)
            slug_str = market_slugs[0][:30]
            filename_parts.append(slug_str)
        
        if start and end:
            filename_parts.append(f"{start}_{end}")
        elif start:
            filename_parts.append(f"from_{start}")
        elif end:
            filename_parts.append(f"until_{end}")
        
        filename = "_".join(filename_parts) + ".parquet"
    
    filepath = DATA_DIR / filename
    
    df.to_parquet(filepath, index=False)
    
    print(f"Saved {len(df)} trades to: {filepath}")
    print(f"File size: {filepath.stat().st_size / 1024:.2f} KB")
    
    return filepath


def display_trade_summary(df):
    """
    Display summary statistics of trades
    """
    if df.empty:
        print("No trades to display")
        return
    
    print("\n" + "="*80)
    print("TRADE SUMMARY")
    print("="*80)
    
    print(f"\nTotal trades: {len(df)}")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"\nUnique markets: {df['slug'].nunique()}")
    
    print("\n--- By Side ---")
    print(df['side'].value_counts())
    
    print("\n--- By Market ---")
    print(df.groupby('slug').agg({
        'size': 'sum',
        'value': 'sum',
        'price': 'mean'
    }).round(4))
    
    print("\n--- Price Statistics ---")
    print(df['price'].describe().round(4))
    
    print("\n--- First 5 Trades ---")
    print(df[['timestamp', 'slug', 'side', 'price', 'size', 'outcome']].head())
    
    print("\n--- Last 5 Trades ---")
    print(df[['timestamp', 'slug', 'side', 'price', 'size', 'outcome']].tail())
    
    print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Fetch historical trades for a Polymarket wallet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with defaults (just python script.py)
  python polymarket_historical_trades.py
  
  # Fetch all trades for a wallet in a time range (using unix timestamps)
  python polymarket_historical_trades.py --wallet 0x123... --start 1734775200 --end 1734789600
  
  # Fetch trades for specific markets
  python polymarket_historical_trades.py --wallet 0x123... --markets bitcoin-up-or-down eth-price-prediction
  
Unix timestamp converter: https://www.unixtimestamp.com/
        """
    )
    
    parser.add_argument(
        '--wallet',
        default=DEFAULT_WALLET,
        help=f'Wallet address to fetch trades for (default: {DEFAULT_WALLET})'
    )
    parser.add_argument(
        '--markets',
        nargs='+',
        default=DEFAULT_MARKETS,
        help=f'List of market slugs to filter (default: {DEFAULT_MARKETS})'
    )
    parser.add_argument(
        '--start',
        type=int,
        default=DEFAULT_START,
        help=f'Start unix timestamp in seconds (default: {DEFAULT_START})'
    )
    parser.add_argument(
        '--end',
        type=int,
        default=DEFAULT_END,
        help=f'End unix timestamp in seconds (default: {DEFAULT_END})'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f'Maximum number of pages to fetch (default: {DEFAULT_MAX_PAGES})'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        default=not DEFAULT_SAVE,
        help='Do not save trades to file'
    )
    parser.add_argument(
        '--load',
        type=str,
        help='Load and display trades from existing Parquet file'
    )
    
    # Use parse_known_args to ignore Jupyter kernel arguments
    args, unknown = parser.parse_known_args()
    
    # Handle loading existing data
    if args.load:
        print(f"Loading trades from: {args.load}")
        df = pd.read_parquet(args.load)
        print(f"\nLoaded {len(df)} trades")
        display_trade_summary(df)
        return
    
    # Fetch trades
    trades = fetch_all_trades(
        user=args.wallet,
        market_slugs=args.markets,
        start=args.start,
        end=args.end,
        max_pages=args.max_pages
    )
    
    if not trades:
        print("No trades found")
        return
    
    # Convert to DataFrame
    df = trades_to_dataframe(trades, args.markets)
    
    # Display summary
    display_trade_summary(df)
    
    # Save to file
    if not args.no_save:
        save_trades_to_parquet(df, args.wallet, args.markets, args.start, args.end, OUTPUT_FILENAME)
        print("\nTo load this data later, use:")
        print(f"python polymarket_historical_trades.py --load polymarket_data/trades_*.parquet")


if __name__ == "__main__":
    main()