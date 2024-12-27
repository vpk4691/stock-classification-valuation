import os
import pandas as pd
from datetime import datetime
from yahoo_finance_collector import YahooFinanceCollector

def create_directory(directory: str) -> None:
    """
    Create directory if it doesn't exist.
    
    Args:
        directory (str): Path to directory to create
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def save_fundamental_data(collector: YahooFinanceCollector, symbol: str, base_filename: str) -> None:
    """
    Collect and save fundamental data for a given stock.
    
    Args:
        collector: YahooFinanceCollector instance
        symbol (str): Stock symbol (e.g., "RELIANCE.NS")
        base_filename (str): Base path for saving files
    """
    print(f"\nFetching fundamental data for {symbol}...")
    fund_data = collector.get_fundamental_data(symbol)
    
    # Ensure directory exists
    directory = os.path.dirname(base_filename)
    create_directory(directory)
    
    # Save each type of fundamental data separately
    for data_type, data in fund_data.items():
        if not data.empty if isinstance(data, pd.DataFrame) else data:
            filename = f"{base_filename}_{data_type}.parquet"
            try:
                collector.save_data(data, filename)
                print(f"Saved {data_type} data for {symbol}")
            except Exception as e:
                print(f"Failed to save {data_type} data for {symbol}: {e}")

def save_historical_data(collector: YahooFinanceCollector, symbol: str, base_filename: str) -> None:
    """
    Collect and save historical data for different timeframes.
    
    Args:
        collector: YahooFinanceCollector instance
        symbol (str): Stock symbol (e.g., "RELIANCE.NS")
        base_filename (str): Base path for saving files
    """
    print(f"\nFetching historical data for {symbol}...")
    
    # Ensure directory exists
    directory = os.path.dirname(base_filename)
    create_directory(directory)
    
    # Define different timeframes for data collection
    timeframes = {
        "1y": "1d",     # 1 year of daily data
        "5y": "1wk",    # 5 years of weekly data
        "max": "1mo"    # Max period with monthly data
    }
    
    # Collect and save data for each timeframe
    for period, interval in timeframes.items():
        try:
            hist_data = collector.get_historical_data(symbol, period=period, interval=interval)
            if not hist_data.empty:
                filename = f"{base_filename}_historical_{period}_{interval}.parquet"
                collector.save_data(hist_data, filename)
                print(f"Saved {period} historical data for {symbol}")
        except Exception as e:
            print(f"Failed to save {period} historical data for {symbol}: {e}")
            
            
            
            
# Uncomment the following after modifying the data_validator.py file
# from data_validator import DataValidator

# def validate_collected_data(data_dict):
#     validator = DataValidator()
#     validation_results = validator.run_all_validations(data_dict)
    
#     # Print validation results
#     for data_type, results in validation_results.items():
#         print(f"\nValidation results for {data_type}:")
#         print(f"Status: {'✓' if results['status'] else '✗'}")
#         if not results['status']:
#             print("Issues found:", results['issues'])
#         if 'freshness' in results:
#             print("Freshness:", results['freshness']['message'])

# # Use in your data collection process
# collected_data = {
#     'historical': historical_df,
#     'balance_sheet': balance_sheet_df,
#     # ... other data ...
# }
# validate_collected_data(collected_data)





def main():
    # Initialize the collector
    collector = YahooFinanceCollector()
    
    # Set up paths
    current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current directory
    repo_root = os.path.dirname(current_dir)                  # Go up one level
    data_dir = os.path.join(repo_root, "data")               # Path to data directory
    
    # Create timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # List of stocks to collect data for
    stocks = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]
    
    # Collect data for each stock
    for symbol in stocks:
        try:
            # Create base filename for this stock
            base_filename = os.path.join(
                data_dir, 
                timestamp, 
                symbol.replace('.NS', '')
            )
            
            # Collect fundamental and historical data
            save_fundamental_data(collector, symbol, base_filename)
            save_historical_data(collector, symbol, base_filename)
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    print("\nData collection complete!")

if __name__ == "__main__":
    main()