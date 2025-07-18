import os
import pandas as pd
import time
import schedule
from datetime import datetime, timedelta
import logging
from hyperliquid.info import Info
import traceback
import requests

# Configuration
# GitHub Actions compatibility
if os.getenv('GITHUB_ACTIONS'):
    DOWNLOADS_FOLDER = os.getenv('DATA_FOLDER', './data')
else:
    DOWNLOADS_FOLDER = os.getenv('DATA_FOLDER', r'C:\Users\quinn\Downloads')

# For GitHub Actions or other environments, use local data folder
if not os.path.exists(DOWNLOADS_FOLDER):
    DOWNLOADS_FOLDER = os.path.join(os.getcwd(), 'data')
    
BASE_URL = "https://api.hyperliquid.xyz"
INTERVAL = "30m"  # 30-minute intervals
HISTORICAL_DAYS = 365  # Changed from 30 to 365 days

# Full list of assets to track (from your script output)
ASSETS = [
    "AAVE", "ACH", "ADA", "ALGO", "APE", "APT", "ARB", "AR", "ATOM", "AVAX", 
    "AXS", "BAKE", "BCH", "BNB", "BONK", "BTC", "CAKE", "CELO", "CFX", "CHZ",
    "COMP", "CRV", "DOGE", "DOT", "DYDX", "EGLD", "ENA", "ENJ", "ENS", "ETC",
    "ETH", "FET", "FIL", "FLM", "FLOKI", "FTT", "FXS", "GALA", "GMT", "GRT",
    "HBAR", "ICP", "ICX", "IMX", "INJ", "IOTA", "IOTX", "JASMY", "KAVA", "LDO",
    "LINK", "LRC", "LTC", "MASK", "MINA", "MKR", "NEAR", "NEO", "OM", "OP",
    "ORDI", "PAXG", "PENDLE", "PEOPLE", "PEPE", "PYTH", "QNT", "RAD", "RARE",
    "RAY", "ROSE", "RSR", "RUNE", "SAND", "SEI", "SHIB", "SOL", "SUI", "TAO",
    "TIA", "TON", "TRB", "TRX", "UNI", "VET", "WIF", "WLD", "XLM", "XRP",
    "XVG", "YGG", "ZEC", "ZRX"
]

# Hyperliquid symbol mapping (handles k-prefix assets)
HYPERLIQUID_SYMBOL_MAP = {
    "PEPE": "kPEPE",
    "SHIB": "kSHIB", 
    "FLOKI": "kFLOKI",
    "BONK": "kBONK",
    # Add other mappings if needed
}

def get_hyperliquid_symbol(asset):
    """Get the correct symbol for Hyperliquid API"""
    return HYPERLIQUID_SYMBOL_MAP.get(asset, asset)

# Setup logging without emojis to avoid Unicode errors
log_file = os.path.join(DOWNLOADS_FOLDER, 'hl_ohlc_puller.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class HyperliquidOHLCPuller:
    def __init__(self):
        self.info = Info(BASE_URL)
        self.downloads_folder = DOWNLOADS_FOLDER
        self.available_symbols = None
        
        # Ensure downloads folder exists
        if not os.path.exists(self.downloads_folder):
            os.makedirs(self.downloads_folder)
            
        logging.info(f"Initialized Hyperliquid OHLC Puller")
        logging.info(f"Downloads folder: {self.downloads_folder}")
        logging.info(f"Tracking {len(ASSETS)} assets")
        logging.info(f"Historical data period: {HISTORICAL_DAYS} days")
        
        # Get available symbols from exchange
        self.get_available_symbols()
        
    def get_available_symbols(self):
        """Get list of available symbols from Hyperliquid"""
        try:
            logging.info("Fetching available symbols from Hyperliquid...")
            
            # Get market metadata
            meta = self.info.meta()
            
            if meta and 'universe' in meta:
                self.available_symbols = set()
                for asset_info in meta['universe']:
                    if 'name' in asset_info:
                        self.available_symbols.add(asset_info['name'])
                
                logging.info(f"Found {len(self.available_symbols)} available symbols")
                
                # Check which of our assets are available
                available_count = 0
                missing_assets = []
                
                for asset in ASSETS:
                    hl_symbol = get_hyperliquid_symbol(asset)
                    if hl_symbol in self.available_symbols:
                        available_count += 1
                    else:
                        missing_assets.append(f"{asset} ({hl_symbol})")
                
                logging.info(f"Available assets: {available_count}/{len(ASSETS)}")
                if missing_assets:
                    logging.warning(f"Missing assets: {missing_assets}")
                    
            else:
                logging.error("Failed to get market metadata")
                self.available_symbols = set()
                
        except Exception as e:
            logging.error(f"Error getting available symbols: {str(e)}")
            logging.error(traceback.format_exc())
            self.available_symbols = set()
    
    def is_symbol_available(self, asset):
        """Check if a symbol is available on Hyperliquid"""
        if self.available_symbols is None:
            return True  # If we couldn't get the list, assume it's available
        
        hl_symbol = get_hyperliquid_symbol(asset)
        return hl_symbol in self.available_symbols
        
    def get_file_path(self, asset):
        """Get the file path for an asset's OHLC data"""
        filename = f"{asset}_ohlc_30.csv"
        return os.path.join(self.downloads_folder, filename)
    
    def load_existing_data(self, asset):
        """Load existing data for an asset"""
        file_path = self.get_file_path(asset)
        
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                # Parse timestamp with explicit format to ensure proper date/time handling
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                return df
            except Exception as e:
                logging.warning(f"Error loading existing data for {asset}: {str(e)}")
                return None
        return None
    
    def get_latest_timestamp(self, existing_df):
        """Get the latest timestamp from existing data"""
        if existing_df is not None and len(existing_df) > 0:
            return existing_df['timestamp'].max()
        return None
    
    def should_rebuild_data(self, asset):
        """Check if existing data should be rebuilt (if it only has limited historical data)"""
        existing_data = self.load_existing_data(asset)
        
        if existing_data is None or len(existing_data) == 0:
            logging.info(f"{asset}: No existing data, will fetch {HISTORICAL_DAYS} days")
            return True  # No data, need to build
        
        # Calculate the date range of existing data
        min_date = existing_data['timestamp'].min()
        max_date = existing_data['timestamp'].max()
        date_range = (max_date - min_date).days
        
        # Calculate how far back the data goes from today
        days_from_today = (datetime.now() - min_date).days
        
        # If data spans less than 250 days OR doesn't go back far enough, rebuild
        # (250 days accounts for weekends, holidays, market closures, etc.)
        if date_range < 250:
            logging.info(f"{asset}: Existing data spans only {date_range} days, rebuilding for {HISTORICAL_DAYS} days")
            return True
        elif days_from_today < 300:
            logging.info(f"{asset}: Data only goes back {days_from_today} days from today, rebuilding for {HISTORICAL_DAYS} days")
            return True
        
        logging.info(f"{asset}: Existing data spans {date_range} days (goes back {days_from_today} days), keeping and updating")
        return False
    
    def fetch_candle_data_chunk(self, asset, start_time_ms, end_time_ms):
        """Fetch a single chunk of candle data"""
        hl_symbol = get_hyperliquid_symbol(asset)
        
        try:
            api_url = f"{BASE_URL}/info"
            
            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": hl_symbol,
                    "interval": INTERVAL,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms
                }
            }
            
            response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
            
            if response.status_code == 200:
                candles = response.json()
                if candles and isinstance(candles, list):
                    return candles
                else:
                    return []
            else:
                logging.error(f"HTTP error for {asset}: {response.status_code} - {response.text}")
                return None
                    
        except Exception as e:
            logging.error(f"HTTP request failed for {asset}: {str(e)}")
            return None

    def fetch_candle_data(self, asset, start_time=None, force_full_history=False):
        """Fetch candle data from Hyperliquid using chunked requests to bypass API limits"""
        try:
            hl_symbol = get_hyperliquid_symbol(asset)
            
            # Check if symbol is available
            if not self.is_symbol_available(asset):
                logging.warning(f"Symbol {asset} ({hl_symbol}) not available on Hyperliquid")
                return None
            
            # Calculate start time
            if start_time is None or force_full_history:
                # Get full historical data
                start_time = datetime.now() - timedelta(days=HISTORICAL_DAYS)
                logging.info(f"Fetching {asset} ({hl_symbol}) - FULL {HISTORICAL_DAYS} days from {start_time}")
            else:
                logging.info(f"Fetching {asset} ({hl_symbol}) - UPDATE from {start_time}")
            
            end_time = datetime.now()
            
            # For full history, use chunked requests to bypass API limits
            if force_full_history or (start_time and (end_time - start_time).days > 50):
                logging.info(f"Using chunked requests for {asset} to get full historical data...")
                
                # Split into 45-day chunks (to stay safely under API limits)
                chunk_days = 45
                all_candles = []
                current_start = start_time
                chunk_count = 0
                
                while current_start < end_time:
                    chunk_count += 1
                    current_end = min(current_start + timedelta(days=chunk_days), end_time)
                    
                    start_time_ms = int(current_start.timestamp() * 1000)
                    end_time_ms = int(current_end.timestamp() * 1000)
                    
                    logging.info(f"  Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
                    
                    chunk_candles = self.fetch_candle_data_chunk(asset, start_time_ms, end_time_ms)
                    
                    if chunk_candles is None:
                        logging.error(f"Failed to fetch chunk {chunk_count} for {asset}")
                        break
                    elif len(chunk_candles) > 0:
                        all_candles.extend(chunk_candles)
                        logging.info(f"  Got {len(chunk_candles)} candles from chunk {chunk_count}")
                    else:
                        logging.info(f"  No data in chunk {chunk_count}")
                    
                    # Move to next chunk
                    current_start = current_end
                    
                    # Small delay between chunks to be respectful to the API
                    time.sleep(0.5)
                
                candles = all_candles
                logging.info(f"Total candles fetched for {asset}: {len(candles)} from {chunk_count} chunks")
                
            else:
                # Single request for smaller time ranges
                start_time_ms = int(start_time.timestamp() * 1000)
                end_time_ms = int(end_time.timestamp() * 1000)
                
                candles = self.fetch_candle_data_chunk(asset, start_time_ms, end_time_ms)
                
                if candles is None:
                    return None
                
                logging.info(f"Successfully fetched {len(candles)} candles for {asset}")
            
            if not candles:
                logging.warning(f"No candle data returned for {asset}")
                return None
            
            # Convert to DataFrame using the working format from the test
            df_data = []
            for candle in candles:
                try:
                    if isinstance(candle, dict):
                        # Use the exact field mapping that worked in the test
                        # Format: {'t': start_time, 'T': end_time, 's': symbol, 'i': interval, 
                        #          'o': open, 'c': close, 'h': high, 'l': low, 'v': volume, 'n': trades}
                        
                        # Convert timestamp to datetime with proper formatting
                        timestamp = pd.to_datetime(int(candle['T']), unit='ms')  # Use 'T' (end time)
                        open_price = float(candle['o'])
                        high_price = float(candle['h'])
                        low_price = float(candle['l'])
                        close_price = float(candle['c'])
                        volume = float(candle['v'])

                        df_data.append({
                            'timestamp': timestamp,
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'volume': volume,
                            'asset': asset,
                            'hl_symbol': hl_symbol
                        })
                        
                except Exception as e:
                    logging.warning(f"Error processing candle for {asset}: {str(e)}")
                    continue
            
            if not df_data:
                logging.warning(f"No valid candle data processed for {asset}")
                return None
            
            df = pd.DataFrame(df_data)
            # Remove duplicates and sort
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Calculate actual date range
            if len(df) > 0:
                date_range = (df['timestamp'].max() - df['timestamp'].min()).days
                logging.info(f"Successfully processed {len(df)} candles for {asset} spanning {date_range} days")
            else:
                logging.info(f"Successfully processed {len(df)} candles for {asset}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error fetching candle data for {asset}: {str(e)}")
            logging.error(traceback.format_exc())
            return None
    
    def merge_and_save_data(self, asset, new_data, replace_existing=False):
        """Merge new data with existing data and save"""
        try:
            if replace_existing:
                # Replace existing data entirely
                combined_data = new_data
                logging.info(f"Replaced data for {asset}: {len(combined_data)} candles")
            else:
                # Merge with existing data
                existing_data = self.load_existing_data(asset)
                
                if existing_data is not None:
                    # Merge data, avoiding duplicates
                    combined_data = pd.concat([existing_data, new_data], ignore_index=True)
                    combined_data = combined_data.drop_duplicates(subset=['timestamp'], keep='last')
                    combined_data = combined_data.sort_values('timestamp').reset_index(drop=True)
                    
                    logging.info(f"Merged data for {asset}: {len(existing_data)} existing + {len(new_data)} new = {len(combined_data)} total")
                else:
                    combined_data = new_data
                    logging.info(f"New data file for {asset}: {len(combined_data)} candles")
            
            # Save to CSV with proper timestamp formatting
            file_path = self.get_file_path(asset)
            # Ensure timestamp is properly formatted before saving
            combined_data['timestamp'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            combined_data.to_csv(file_path, index=False)
            
            # Log file size and date range
            if len(combined_data) > 0:
                start_date = combined_data['timestamp'].min()
                end_date = combined_data['timestamp'].max()
                logging.info(f"Saved {asset} data: {len(combined_data)} candles from {start_date} to {end_date}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error saving data for {asset}: {str(e)}")
            return False
    
    def update_single_asset(self, asset):
        """Update data for a single asset with smart rebuilding"""
        try:
            # Check if symbol is available first
            if not self.is_symbol_available(asset):
                logging.warning(f"Skipping {asset} - not available on Hyperliquid")
                return False
            
            # Check if we should rebuild the data (for short datasets)
            should_rebuild = self.should_rebuild_data(asset)
            
            if should_rebuild:
                # Rebuild with full historical data
                new_data = self.fetch_candle_data(asset, force_full_history=True)
                
                if new_data is not None and len(new_data) > 0:
                    # Replace existing data entirely
                    if self.merge_and_save_data(asset, new_data, replace_existing=True):
                        logging.info(f"SUCCESS: Rebuilt {asset} with {HISTORICAL_DAYS} days of data")
                        return True
                    else:
                        logging.error(f"FAILED: Could not save rebuilt data for {asset}")
                        return False
                else:
                    logging.warning(f"WARNING: No historical data available for {asset}")
                    return False
            else:
                # Normal update - just get recent data
                existing_data = self.load_existing_data(asset)
                latest_timestamp = self.get_latest_timestamp(existing_data)
                
                if latest_timestamp:
                    # Start from the last timestamp to ensure we don't miss any data
                    start_time = latest_timestamp
                    logging.info(f"Updating {asset} from {start_time}")
                else:
                    # No existing data, get full history
                    start_time = datetime.now() - timedelta(days=HISTORICAL_DAYS)
                    logging.info(f"Creating new data file for {asset} from {start_time}")
                
                # Fetch new data
                new_data = self.fetch_candle_data(asset, start_time)
                
                if new_data is not None and len(new_data) > 0:
                    # Save the data
                    if self.merge_and_save_data(asset, new_data):
                        logging.info(f"SUCCESS: Updated {asset}")
                        return True
                    else:
                        logging.error(f"FAILED: Could not save data for {asset}")
                        return False
                else:
                    logging.warning(f"WARNING: No new data for {asset}")
                    return False
                
        except Exception as e:
            logging.error(f"ERROR: Updating {asset}: {str(e)}")
            return False
    
    def update_all_assets(self):
        """Update data for all assets"""
        start_time = datetime.now()
        logging.info(f"Starting update cycle for {len(ASSETS)} assets at {start_time}")
        
        success_count = 0
        fail_count = 0
        rebuild_count = 0
        
        for i, asset in enumerate(ASSETS, 1):
            try:
                logging.info(f"Processing {asset} ({i}/{len(ASSETS)})")
                
                # Check if this asset needs rebuilding
                needs_rebuild = self.should_rebuild_data(asset)
                if needs_rebuild:
                    rebuild_count += 1
                
                if self.update_single_asset(asset):
                    success_count += 1
                else:
                    fail_count += 1
                    
                # Delay between requests to avoid rate limiting
                time.sleep(1.0)
                
            except Exception as e:
                logging.error(f"Unexpected error processing {asset}: {str(e)}")
                fail_count += 1
                # Continue with next asset
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"Update cycle completed in {duration}")
        logging.info(f"   SUCCESS: {success_count}")
        logging.info(f"   FAILED: {fail_count}")
        logging.info(f"   REBUILT: {rebuild_count}")
        if success_count + fail_count > 0:
            logging.info(f"   Success rate: {(success_count/(success_count+fail_count)*100):.1f}%")
        
        return success_count, fail_count
    
    def verify_data_integrity(self):
        """Verify the integrity of saved data files"""
        logging.info("Verifying data integrity...")
        
        for asset in ASSETS:
            file_path = self.get_file_path(asset)
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    
                    if len(df) > 0:
                        # Check for required columns
                        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        
                        if missing_cols:
                            logging.warning(f"WARNING {asset}: Missing columns {missing_cols}")
                        else:
                            # Check for data gaps
                            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                            df = df.sort_values('timestamp')
                            
                            # Calculate date range
                            date_range = (df['timestamp'].max() - df['timestamp'].min()).days
                            
                            # Check for 30-minute intervals
                            time_diffs = df['timestamp'].diff()
                            expected_diff = timedelta(minutes=30)
                            
                            gaps = time_diffs[time_diffs > expected_diff * 1.5]  # Allow some tolerance
                            
                            if len(gaps) > 0:
                                logging.warning(f"WARNING {asset}: Found {len(gaps)} data gaps")
                            
                            logging.info(f"VERIFIED {asset}: {len(df)} candles, {date_range} days, {df['timestamp'].min()} to {df['timestamp'].max()}")
                    else:
                        logging.warning(f"WARNING {asset}: Empty data file")
                        
                except Exception as e:
                    logging.error(f"ERROR verifying {asset}: {str(e)}")
            else:
                logging.warning(f"WARNING {asset}: Data file not found")

def run_update_cycle():
    """Run a single update cycle"""
    try:
        puller = HyperliquidOHLCPuller()
        puller.update_all_assets()
        
        # Verify data integrity every few cycles
        if datetime.now().hour % 6 == 0 and datetime.now().minute < 30:
            puller.verify_data_integrity()
            
    except Exception as e:
        logging.error(f"Error in update cycle: {str(e)}")
        logging.error(traceback.format_exc())

def run_initial_setup():
    """Run initial setup to create data files"""
    logging.info("Running initial setup...")
    
    try:
        puller = HyperliquidOHLCPuller()
        puller.update_all_assets()
        puller.verify_data_integrity()
        
        logging.info("Initial setup completed")
        
    except Exception as e:
        logging.error(f"Error in initial setup: {str(e)}")
        logging.error(traceback.format_exc())

def main():
    """Main function to run the scheduler"""
    logging.info("Starting Hyperliquid OHLC Data Puller")
    logging.info(f"Tracking {len(ASSETS)} assets")
    logging.info(f"Historical period: {HISTORICAL_DAYS} days")
    logging.info(f"Saving to: {DOWNLOADS_FOLDER}")

    # Check if running in GitHub Actions
    if os.getenv('GITHUB_ACTIONS'):
        logging.info("Running in GitHub Actions - single run mode")
        try:
            puller = HyperliquidOHLCPuller()
            
            # Check if we need initial setup (no existing data files)
            existing_files = [f for f in os.listdir(DOWNLOADS_FOLDER) if f.endswith('_ohlc_30.csv')]
            if len(existing_files) == 0:
                logging.info("No existing data found - running initial setup...")
                puller.update_all_assets()
                puller.verify_data_integrity()
            else:
                logging.info(f"Found {len(existing_files)} existing data files - running update cycle...")
                puller.update_all_assets()
                
            logging.info("GitHub Actions run completed successfully")
            return
            
        except Exception as e:
            logging.error(f"Error in GitHub Actions run: {str(e)}")
            logging.error(traceback.format_exc())
            import sys
            sys.exit(1)
    
    # Check if running in automated mode (for local use)
    import sys
    automated_mode = '--auto' in sys.argv or os.getenv('AUTO_MODE', '').lower() == 'true'

    if automated_mode:
        logging.info("Running in automated mode - starting continuous scheduler...")
        logging.info(f"Update interval: Every 30 minutes")
        try:
            # Run initial setup if no data exists
            puller = HyperliquidOHLCPuller()

            # Check if we need initial setup (no existing data files)
            existing_files = [f for f in os.listdir(DOWNLOADS_FOLDER) if f.endswith('_ohlc_30.csv')]
            if len(existing_files) == 0:
                logging.info("No existing data found - running initial setup...")
                run_initial_setup()
            else:
                logging.info(f"Found {len(existing_files)} existing data files - running update cycle...")
                run_update_cycle()

            # Schedule regular updates every 30 minutes
            schedule.every(30).minutes.do(run_update_cycle)

            logging.info("Automated scheduler started - will update every 30 minutes")
            logging.info("Running continuously... (Ctrl+C to stop)")

            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logging.info("Automated mode stopped by user")
        except Exception as e:
            logging.error(f"Error in automated mode: {str(e)}")
            logging.error(traceback.format_exc())
    else:
        # Interactive mode
        print("\nChoose an option:")
        print("1. Run initial setup (get 365 days of historical data)")
        print("2. Run single update cycle")
        print("3. Run continuous scheduler (every 30 minutes)")
        print("4. Verify existing data integrity")
        print("5. Run automated mode (initial setup + continuous)")
        print("6. Force rebuild all data (365 days)")

        try:
            choice = input("Enter choice (1-6): ").strip()

            if choice == "1":
                logging.info("Running initial setup...")
                run_initial_setup()
            elif choice == "2":
                logging.info("Running single update cycle...")
                run_update_cycle()
            elif choice == "3":
                logging.info("Starting continuous scheduler...")

                # Run initial update
                run_update_cycle()

                # Schedule regular updates every 30 minutes
                schedule.every(30).minutes.do(run_update_cycle)

                logging.info("Scheduler started - will update every 30 minutes")
                logging.info("Press Ctrl+C to stop")

                while True:
                    schedule.run_pending()
                    time.sleep(60)  # Check every minute
            elif choice == "4":
                logging.info("Verifying data integrity...")
                puller = HyperliquidOHLCPuller()
                puller.verify_data_integrity()
            elif choice == "5":
                logging.info("Starting automated mode...")
                # Restart in automated mode
                os.execv(sys.executable, [sys.executable] + sys.argv + ['--auto'])
            elif choice == "6":
                logging.info("Force rebuilding all data with 365 days...")
                puller = HyperliquidOHLCPuller()
                # Force rebuild all by temporarily making should_rebuild_data return True
                original_method = puller.should_rebuild_data
                puller.should_rebuild_data = lambda asset: True
                puller.update_all_assets()
                puller.should_rebuild_data = original_method
            else:
                logging.error("Invalid choice")

        except KeyboardInterrupt:
            logging.info("Stopped by user")
        except Exception as e:
            logging.error(f"Error: {str(e)}")
            logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()
