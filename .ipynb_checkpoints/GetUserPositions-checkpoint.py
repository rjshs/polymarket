import requests
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from datetime import datetime
import time

# Your Polygonscan API key
POLYGONSCAN_API_KEY = "4W4KUG6RPKGX2WBPZQXI9WU2XE5H3P2YKJ"  # Replace with your actual key

# Polymarket's CTF Exchange contract address on Polygon
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

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

def get_current_positions(wallet_address):
    """
    Get current positions using Polymarket's Data API
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
        print(f"Found {len(positions)} current positions")
        return positions
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return []

def display_positions(positions):
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
        print(f"Outcome: {pos['outcome']}")
        print(f"Shares: {pos['size']:.2f}")
        print(f"Avg Price: ${pos['avgPrice']:.4f}")
        print(f"Current Price: ${pos['curPrice']:.4f}")
        print(f"Current Value: ${pos['currentValue']:.2f}")
        print(f"P&L: ${pos['cashPnl']:.2f} ({pos['percentPnl']:.2f}%)")
        print("-"*80)

def monitor_wallet(wallet_address, interval=60):
    """
    Continuously monitor a wallet's positions
    """
    print(f"Starting real-time monitoring for {wallet_address}")
    print(f"Updating every {interval} seconds. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            positions = get_current_positions(wallet_address)
            display_positions(positions)
            print(f"\nLast updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Next update in {interval} seconds...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")

def main(wallet_addresses=None, skip_leaderboard=False, top_volume=False, 
         top_profit=False, plot=True, latest_price_mode=False, monitor=False,
         interval=60):
    """
    Main function to analyze Polymarket user positions
    """
    
    if not wallet_addresses:
        print("No wallet address provided. Please use --wallets flag.")
        return
    
    for wallet in wallet_addresses:
        print(f"\n{'='*80}")
        print(f"Analyzing wallet: {wallet}")
        print(f"{'='*80}\n")
        
        if monitor:
            monitor_wallet(wallet, interval)
        else:
            # Get current positions
            positions = get_current_positions(wallet)
            display_positions(positions)
            
            # Optionally fetch blockchain transaction history
            if not latest_price_mode:
                print("\nFetching blockchain transaction history...")
                transactions = fetch_user_transactions(wallet)
                if not transactions.empty:
                    print(f"Total blockchain transactions: {len(transactions)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Track Polymarket user positions in real-time'
    )
    
    parser.add_argument(
        '--wallets',
        nargs='+',
        help='List of wallet addresses to track'
    )
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Enable continuous monitoring mode'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Update interval in seconds for monitor mode (default: 60)'
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
        skip_leaderboard=args.skip_leaderboard,
        top_volume=args.top_volume,
        top_profit=args.top_profit,
        plot=not args.no_plot,
        latest_price_mode=args.latest_price_mode,
        monitor=args.monitor,
        interval=args.interval
    )