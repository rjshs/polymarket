import requests
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from datetime import datetime
import time
import json
import os
from pathlib import Path

# Your Polygonscan API key
POLYGONSCAN_API_KEY = "4W4KUG6RPKGX2WBPZQXI9WU2XE5H3P2YKJ"  # Replace with your actual key

# Polymarket's CTF Exchange contract address on Polygon
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

# Data storage directory
DATA_DIR = Path("polymarket_data")
DATA_DIR.mkdir(exist_ok=True)


def extract_market_slug(market_url):
    """
    Extract the market slug from a Polymarket URL
    Examples:
    - https://polymarket.com/event/bitcoin-up-or-down-december-21-3am-et -> bitcoin-up-or-down-december-21-3am-et
    - bitcoin-up-or-down-december-21-3am-et -> bitcoin-up-or-down-december-21-3am-et
    """
    if 'polymarket.com' in market_url:
        # Extract slug from URL
        parts = market_url.rstrip('/').split('/')
        return parts[-1]
    else:
        # Already a slug
        return market_url.strip()


def get_market_info(market_slug):
    """
    Get market information from Polymarket API to find condition IDs
    """
    print(f"Fetching market info for: {market_slug}")
    
    # Try the Gamma API to get market details
    url = f"https://gamma-api.polymarket.com/events/{market_slug}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            event_data = response.json()
            
            # Extract condition IDs from all markets in this event
            condition_ids = []
            market_info = {}
            
            for market in event_data.get('markets', []):
                condition_id = market.get('conditionId')
                if condition_id:
                    condition_ids.append(condition_id)
                    market_info[condition_id] = {
                        'question': market.get('question'),
                        'outcomes': market.get('outcomes', []),
                        'slug': market_slug
                    }
            
            print(f"We found {len(condition_ids)} markets in event: {event_data.get('title')}")
            return condition_ids, market_info
        else:
            print(f"Error fetching market info: {response.status_code}")
            return [], {}
    except Exception as e:
        print(f"Error: {e}")
        return [], {}


def fetch_user_transactions(wallet_address):
    """
    Fetch ERC-1155 token transfers for a wallet from Polygonscan
    """
    print(f"Fetching transactions for wallet: {wallet_address}")
    
    url = f"https://api.polygonscan.com/api"
    params = {
        'module': 'account',
        'action': 'token1155tx',
        'address': wallet_address,
        'startblock': 0,
        'endblock': 99999999,
        'sort': 'asc',
        'apikey': POLYGONSCAN_API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if data['status'] == '1':
        print(f"Found {len(data['result'])} transactions")
        return pd.DataFrame(data['result'])
    else:
        print(f"Error: {data['message']}")
        return pd.DataFrame()


def get_current_positions(wallet_address, condition_ids=None, market_filter=None):
    """
    Get current positions using Polymarket's Data API
    Optionally filter by specific condition IDs or market criteria
    """
    print(f"Fetching current positions from Polymarket API...")
    
    url = f"https://data-api.polymarket.com/positions"
    params = {
        'user': wallet_address,
        'limit': 500
    }
    
    try:
        response = requests.get(url, params=params)
        positions = response.json()
        
        # Filter by condition IDs if provided
        if condition_ids:
            positions = [p for p in positions if p.get('conditionId') in condition_ids]
            print(f"Found {len(positions)} positions matching specified condition IDs")
        
        # Additional filtering by market characteristics
        if market_filter:
            original_count = len(positions)
            
            # Filter by event slug if specified
            if 'event_slugs' in market_filter:
                positions = [p for p in positions if p.get('eventSlug') in market_filter['event_slugs']]
            
            # Filter by active/not redeemable (exclude expired markets)
            if market_filter.get('active_only'):
                positions = [p for p in positions if not p.get('redeemable', False)]
            
            # Filter by end date (only future markets)
            if market_filter.get('future_only'):
                now = datetime.now().isoformat()
                positions = [p for p in positions if p.get('endDate', '') > now]
            
            print(f"Filtered from {original_count} to {len(positions)} positions based on criteria")
        
        if not condition_ids and not market_filter:
            print(f"Found {len(positions)} total positions")
        
        return positions
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return []


def save_position_snapshot(wallet_address, positions, market_slugs=None):
    """
    Save a snapshot of positions to a Parquet file with timestamp
    Parquet is best for time-series data - more efficient than CSV, easier than JSON
    """
    if not positions:
        print("No positions to save")
        return
    
    timestamp = datetime.now()
    
    # Create records with timestamp
    records = []
    for pos in positions:
        record = {
            'timestamp': timestamp,
            'wallet_address': wallet_address,
            'market_title': pos['title'],
            'market_slug': pos['slug'],
            'event_slug': pos['eventSlug'],
            'condition_id': pos['conditionId'],
            'outcome': pos['outcome'],
            'outcome_index': pos['outcomeIndex'],
            'shares': pos['size'],
            'avg_price': pos['avgPrice'],
            'current_price': pos['curPrice'],
            'initial_value': pos['initialValue'],
            'current_value': pos['currentValue'],
            'cash_pnl': pos['cashPnl'],
            'percent_pnl': pos['percentPnl'],
            'total_bought': pos['totalBought'],
            'realized_pnl': pos['realizedPnl'],
            'percent_realized_pnl': pos['percentRealizedPnl'],
            'redeemable': pos['redeemable'],
            'end_date': pos['endDate'],
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Create filename based on wallet and markets
    if market_slugs:
        markets_str = "_".join([slug[:20] for slug in market_slugs[:2]])  # Use first 2 slugs
        filename = f"positions_{wallet_address[:8]}_{markets_str}.parquet"
    else:
        filename = f"positions_{wallet_address[:8]}_all.parquet"
    
    filepath = DATA_DIR / filename
    
    # Append to existing file or create new one
    if filepath.exists():
        existing_df = pd.read_parquet(filepath)
        df = pd.concat([existing_df, df], ignore_index=True)
    
    df.to_parquet(filepath, index=False)
    print(f"Saved snapshot to: {filepath}")
    print(f"Total records in file: {len(df)}")
    
    return filepath


def load_position_history(filepath):
    """
    Load saved position history into a pandas DataFrame
    """
    if not Path(filepath).exists():
        print(f"File not found: {filepath}")
        return None
    
    df = pd.read_parquet(filepath)
    print(f"Loaded {len(df)} records from {filepath}")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    return df


def display_positions(positions, market_info=None):
    """
    Display current positions in a readable format
    """
    if not positions:
        print("No active positions found")
        return
    
    print("\n" + "="*80)
    print("CURRENT POSITIONS")
    print("="*80)
    
    for pos in positions:
        print(f"\nMarket: {pos['title']}")
        
        # Show additional market context if available
        if market_info and pos['conditionId'] in market_info:
            info = market_info[pos['conditionId']]
            print(f"Question: {info['question']}")
        
        print(f"Outcome: {pos['outcome']}")
        print(f"Shares: {pos['size']:.2f}")
        print(f"Avg Price: ${pos['avgPrice']:.4f}")
        print(f"Current Price: ${pos['curPrice']:.4f}")
        print(f"Current Value: ${pos['currentValue']:.2f}")
        print(f"P&L: ${pos['cashPnl']:.2f} ({pos['percentPnl']:.2f}%)")
        print(f"Condition ID: {pos['conditionId']}")
        print("-"*80)


def monitor_wallet(wallet_address, interval=60, condition_ids=None, market_info=None, 
                   market_slugs=None, save_data=True, market_filter=None):
    """
    Continuously monitor a wallet's positions and save data
    """
    print(f"Starting real-time monitoring for {wallet_address}")
    if condition_ids:
        print(f"Filtering for {len(condition_ids)} specific markets")
    if market_filter:
        if market_filter.get('active_only'):
            print("Filtering: Active positions only (excluding expired/redeemed)")
        if market_filter.get('future_only'):
            print("Filtering: Future markets only (excluding past end dates)")
    print(f"Updating every {interval} seconds. Press Ctrl+C to stop.\n")
    
    snapshot_count = 0
    
    try:
        while True:
            positions = get_current_positions(wallet_address, condition_ids, market_filter)
            display_positions(positions, market_info)
            
            # Save snapshot to file
            if save_data and positions:
                filepath = save_position_snapshot(wallet_address, positions, market_slugs)
                snapshot_count += 1
                print(f"\nTotal snapshots saved: {snapshot_count}")
            elif save_data and not positions:
                print("\nNo positions to save (likely all expired)")
            
            print(f"\nLast updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Next update in {interval} seconds...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
        print(f"Total snapshots saved: {snapshot_count}")
        if save_data:
            print(f"\nData saved to: {DATA_DIR}")
            print("Load with: pd.read_parquet('polymarket_data/positions_*.parquet')")


def main(wallet_addresses=None, market_urls=None, skip_leaderboard=False, top_volume=False, 
         top_profit=False, plot=True, latest_price_mode=False, monitor=False,
         interval=60, save_data=True, load_file=None, active_only=False, future_only=False):
    """
    Main function to analyze Polymarket user positions
    """
    
    # Handle loading existing data
    if load_file:
        print(f"Loading data from: {load_file}")
        df = load_position_history(load_file)
        if df is not None:
            print("\nDataFrame Info:")
            print(df.info())
            print("\nFirst few rows:")
            print(df.head())
            print("\nUnique markets:")
            print(df['market_title'].unique())
        return
    
    if not wallet_addresses:
        print("No wallet address provided. Please use --wallets flag.")
        print("\nUsage examples:")
        print("  Monitor active positions in event (filters out expired):")
        print("    python polymarket_tracker.py --wallets 0xADDRESS --markets btc-updown-15m-1766341800 --monitor --active-only")
        print("  Load saved data:")
        print("    python polymarket_tracker.py --load polymarket_data/positions_0x123456_bitcoin.parquet")
        return
    
    # Process market URLs to get condition IDs
    condition_ids = []
    market_info = {}
    market_slugs = []
    
    if market_urls:
        print(f"\nProcessing {len(market_urls)} market URLs...")
        for url in market_urls:
            slug = extract_market_slug(url)
            market_slugs.append(slug)
            cids, minfo = get_market_info(slug)
            condition_ids.extend(cids)
            market_info.update(minfo)
        
        print(f"\nTotal condition IDs found across all events: {len(condition_ids)}")
        if not condition_ids:
            print("Warning: No valid markets found. Will show all positions.")
    
    # Build market filter criteria
    market_filter = None
    if market_slugs or active_only or future_only:
        market_filter = {}
        if market_slugs:
            market_filter['event_slugs'] = market_slugs
        if active_only:
            market_filter['active_only'] = True
        if future_only:
            market_filter['future_only'] = True
    
    for wallet in wallet_addresses:
        print(f"\n{'='*80}")
        print(f"Analyzing wallet: {wallet}")
        if market_slugs:
            print(f"Filtering for event slugs: {market_slugs}")
        if active_only:
            print("Filtering: Active positions only (not expired/redeemed)")
        if future_only:
            print("Filtering: Future markets only")
        print(f"{'='*80}\n")
        
        if monitor:
            # Continuous monitoring mode
            monitor_wallet(wallet, interval, condition_ids, market_info, 
                         market_slugs, save_data, market_filter)
        else:
            # One-time check
            print("Running one-time position check. Use --monitor for continuous updates.\n")
            positions = get_current_positions(wallet, condition_ids, market_filter)
            display_positions(positions, market_info)
            
            # Save snapshot if requested
            if save_data and positions:
                filepath = save_position_snapshot(wallet, positions, market_slugs)
                print(f"\nData saved to: {filepath}")
            
            # Optionally fetch blockchain transaction history
            if not latest_price_mode and POLYGONSCAN_API_KEY != "YOUR_API_KEY_HERE":
                print("\nFetching blockchain transaction history...")
                transactions = fetch_user_transactions(wallet)
                if not transactions.empty:
                    print(f"Total blockchain transactions: {len(transactions)}")
            elif not latest_price_mode:
                print("\nSkipping blockchain history (Polygonscan API key not configured)")
                print("To enable: Set POLYGONSCAN_API_KEY in the script")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Track Polymarket user positions in real-time with data logging',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor specific Bitcoin markets
  python polymarket_tracker.py --wallets 0x123... --markets https://polymarket.com/event/bitcoin-up-or-down-december-21 --monitor --interval 30
  
  # Monitor multiple markets
  python polymarket_tracker.py --wallets 0x123... --markets bitcoin-up-or-down eth-price-prediction --monitor
  
  # One-time check with data save
  python polymarket_tracker.py --wallets 0x123... --markets bitcoin-up-or-down
  
  # Load and analyze saved data
  python polymarket_tracker.py --load polymarket_data/positions_0x6031b6e_bitcoin-up-or-down.parquet
        """
    )
    
    parser.add_argument(
        '--wallets',
        nargs='+',
        help='List of wallet addresses to track'
    )
    parser.add_argument(
        '--markets',
        nargs='+',
        help='List of market URLs or slugs to filter (e.g., https://polymarket.com/event/bitcoin-up-or-down or just bitcoin-up-or-down)'
    )
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Enable continuous monitoring mode'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=60,
        help='Update interval in seconds for monitor mode (default: 60)'
    )
    parser.add_argument(
        '--active-only',
        action='store_true',
        help='Only show active positions (exclude expired/redeemed markets). Useful for 15-min markets.'
    )
    parser.add_argument(
        '--future-only',
        action='store_true',
        help='Only show positions in markets that have not ended yet'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Disable automatic data saving'
    )
    parser.add_argument(
        '--load',
        type=str,
        help='Load and display saved data from a Parquet file'
    )
    parser.add_argument(
        '--skip-leaderboard',
        action='store_true',
        help='Skip leaderboard fetching'
    )
    parser.add_argument(
        '--top-volume',
        action='store_true',
        help='Fetch top volume users'
    )
    parser.add_argument(
        '--top-profit',
        action='store_true',
        help='Fetch top profit users'
    )
    parser.add_argument(
        '--no-plot',
        action='store_true',
        help='Disable plot generation'
    )
    parser.add_argument(
        '--latest-price-mode',
        action='store_true',
        help='Only retrieve the latest prices, no plotting'
    )
    
    args = parser.parse_args()
    
    main(
        wallet_addresses=args.wallets,
        market_urls=args.markets,
        skip_leaderboard=args.skip_leaderboard,
        top_volume=args.top_volume,
        top_profit=args.top_profit,
        plot=not args.no_plot,
        latest_price_mode=args.latest_price_mode,
        monitor=args.monitor,
        interval=args.interval,
        save_data=not args.no_save,
        load_file=args.load,
        active_only=args.active_only,
        future_only=args.future_only
    )