import os
import pandas as pd
import json
from datetime import datetime
from yahoo_finance_collector import YahooFinanceCollector
from data_validator import DataValidator  # Add this import
from data_preprocessing_module import DataPreprocessor


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
    print(f"\nFetching fundamental data for {symbol}...")
    fund_data = collector.get_fundamental_data(symbol)
    
    # Ensure directory exists
    directory = os.path.dirname(base_filename)
    create_directory(directory)
    
    # Save each type of fundamental data separately
    for data_type, data in fund_data.items():
        if not data.empty if isinstance(data, pd.DataFrame) else data:
            # Update the filename format to match what we're looking for later
            data_type_name = data_type.lower().replace(' ', '_')
            filename = f"{base_filename}_{data_type_name}.parquet"
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
            
            
def convert_to_serializable(obj):
    """Convert objects to JSON serializable format"""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, dict):
        return {str(k): convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(i) for i in obj]
    elif isinstance(obj, (pd.Index, pd.Series)):
        return convert_to_serializable(obj.tolist())
    return obj

def validate_collected_data(data_files: dict, validation_dir: str) -> dict:
    """
    Validate collected data files and save validation results
    """
    validator = DataValidator()
    validation_data = {}
    
    # Create validation directory if it doesn't exist
    os.makedirs(validation_dir, exist_ok=True)
    
    # Read and organize data for validation
    for data_type, file_path in data_files.items():
        try:
            df = pd.read_parquet(file_path)
            validation_data[data_type] = df
        except Exception as e:
            print(f"Error reading {data_type} data: {e}")
    
    # Run validations
    validation_results = validator.run_all_validations(validation_data)
    
    # Convert results to serializable format
    serializable_results = convert_to_serializable(validation_results)
    
    # Save validation results
    validation_file = os.path.join(validation_dir, 'validation_results.json')
    with open(validation_file, 'w') as f:
        json.dump(serializable_results, f)
    
    # Save validated data
    for data_type, df in validation_data.items():
        output_file = os.path.join(validation_dir, f"{data_type}_validated.parquet")
        df.to_parquet(output_file)
    
    # Print validation results
    for data_type, results in validation_results.items():
        print(f"\nValidation results for {data_type}:")
        print(f"Status: {'✓' if results['status'] else '✗'}")
        if not results['status']:
            print("Issues found:", results['issues'])
        if 'freshness' in results:
            print("Freshness:", results['freshness']['message'])
            
    return validation_results

def run_preprocessing(validation_dir: str, processed_dir: str):
    """
    Run preprocessing on validated data
    
    Args:
        validation_dir (str): Directory containing validated data
        processed_dir (str): Directory to save processed data
    """
    # Create processed directory
    os.makedirs(processed_dir, exist_ok=True)
    
    # Initialize preprocessor
    preprocessor = DataPreprocessor()
    
    try:
        # Get latest timestamp directory
        timestamp_dirs = [d for d in os.listdir(validation_dir) 
                        if os.path.isdir(os.path.join(validation_dir, d))]
        if not timestamp_dirs:
            raise Exception("No validated data found")
            
        latest_dir = max(timestamp_dirs)
        
        # Process each stock's data
        stock_dirs = os.listdir(os.path.join(validation_dir, latest_dir))
        for stock in stock_dirs:
            print(f"\nProcessing data for {stock}")
            stock_dir = os.path.join(validation_dir, latest_dir, stock)
            
            # Read validated data
            data_dict = {}
            for file in os.listdir(stock_dir):
                if file.endswith('_validated.parquet'):
                    data_type = file.replace('_validated.parquet', '')
                    data_dict[data_type] = pd.read_parquet(os.path.join(stock_dir, file))
            
            # Process data
            processed_data = preprocessor.process_stock_data(data_dict)
            
            # Save processed data
            output_dir = os.path.join(processed_dir, latest_dir, stock)
            os.makedirs(output_dir, exist_ok=True)
            
            for data_type, data in processed_data.items():
                output_file = os.path.join(output_dir, f"{data_type}.parquet")
                if isinstance(data, pd.DataFrame):
                    data.to_parquet(output_file)
                    
    except Exception as e:
        print(f"Error in preprocessing: {e}")


def main():
    # Initialize the collector
    collector = YahooFinanceCollector()
    
    # Set up paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(current_dir)
    raw_data_dir = os.path.join(repo_root, "data", "raw")
    validation_dir = os.path.join(repo_root, "data", "validation")
    processed_dir = os.path.join(repo_root, "data", "processed")
    
     # Create directories
    os.makedirs(raw_data_dir, exist_ok=True)
    os.makedirs(validation_dir, exist_ok=True)
    
    # Create timestamp for this run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # List of stocks to collect data for
    stocks = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]
    
    # Collect and validate data for each stock
    for symbol in stocks:
        try:
            # Create base filename for this stock
            base_filename = os.path.join(
                raw_data_dir, 
                timestamp, 
                symbol.replace('.NS', '')
            )
            
            # Collect fundamental and historical data
            save_fundamental_data(collector, symbol, base_filename)
            save_historical_data(collector, symbol, base_filename)
            
            # Validate collected data
            print(f"\nValidating data for {symbol}...")
            data_files = {
                'balance_sheet': f"{base_filename}_balance_sheet.parquet",
                'income_statement': f"{base_filename}_income_statement.parquet",
                'cash_flow': f"{base_filename}_cash_flow.parquet",
                'historical_1y_1d': f"{base_filename}_historical_1y_1d.parquet",
                'historical_5y_1wk': f"{base_filename}_historical_5y_1wk.parquet",
                'historical_max_1mo': f"{base_filename}_historical_max_1mo.parquet"

                # add validation of other historical data 
            }
            validation_results = validate_collected_data(
                data_files, 
                os.path.join(validation_dir, timestamp, symbol.replace('.NS', ''))
            )
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    print("\nData collection and validation complete!")
    
    # Run preprocessing
    print("\nStarting preprocessing...")
    run_preprocessing(validation_dir, processed_dir)
    print("\nPreprocessing complete!")

    
    
    
    
if __name__ == "__main__":
    main()


