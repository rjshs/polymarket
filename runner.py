import pandas as pd
from pathlib import Path

# ============ CONFIGURATION ============
# Input file (The contaminated one)
INPUT_FILE = "polymarket_data/trades_0x6031b6_1766362500_1766364300.parquet"

# Output file (The clean one)
OUTPUT_FILE = "polymarket_data/trades_0x6031b6_CLEANED_btc_only.parquet"

# Terms to exclude (case-insensitive)
EXCLUDE_TERMS = ["eth", "ethereum"]

def clean_dataframe():
    path = Path(INPUT_FILE)
    if not path.exists():
        print(f"❌ File not found: {INPUT_FILE}")
        return

    print(f"🔄 Loading {INPUT_FILE}...")
    df = pd.read_parquet(path)
    original_count = len(df)
    
    # Create a mask for rows to KEEP
    # We check both 'slug' and 'market_title' just to be safe
    # ~ means "NOT"
    # case=False means "ETH", "eth", "Ethereum" all get caught
    mask = (
        ~df['slug'].str.contains('|'.join(EXCLUDE_TERMS), case=False, na=False) & 
        ~df['market_title'].str.contains('|'.join(EXCLUDE_TERMS), case=False, na=False)
    )
    
    df_clean = df[mask]
    
    # Calculate stats
    removed_count = original_count - len(df_clean)
    
    print("-" * 40)
    print(f"Original Rows: {original_count}")
    print(f"Ethereum Rows Removed: {removed_count}")
    print(f"Clean Rows Remaining: {len(df_clean)}")
    print("-" * 40)

    if removed_count > 0:
        # Save the new file
        df_clean.to_parquet(OUTPUT_FILE, index=False)
        print(f"✅ Clean file saved to:\n   {OUTPUT_FILE}")
        
        # Verify that it really is clean
        print("\n🔍 Verification Check:")
        sample_slugs = df_clean['slug'].unique()
        print(f"Remaining Market Slugs: {sample_slugs}")
    else:
        print("⚠️ No Ethereum rows were found. File was already clean")

if __name__ == "__main__":
    clean_dataframe()