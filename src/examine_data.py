import pandas as pd
import os
from glob import glob
from datetime import datetime

# Find the most recent data directory
data_dir = "../data"  # Adjust if needed
timestamp_dirs = glob(os.path.join(data_dir, "*"))

# Sort directories by name (since they're named with timestamps)
latest_dir = sorted(timestamp_dirs)[-1] if timestamp_dirs else None

if latest_dir:
    print(f"Examining data from directory: {latest_dir}")
    
    # Find all RELIANCE files
    reliance_files = glob(os.path.join(latest_dir, "RELIANCE*"))
    print("reliance_files:",reliance_files)
    # Read and examine each file
    for file in reliance_files:
        print(f"\nExamining file: {os.path.basename(file)}")
        try:
            df = pd.read_parquet(file)
            
            # Handle both DataFrame and Series
            if isinstance(df, (pd.DataFrame, pd.Series)):
                print("Shape:", df.shape if isinstance(df, pd.DataFrame) else df.shape[0])
                if isinstance(df, pd.DataFrame):
                    print("\nColumns:", df.columns.tolist())
                print("\nFirst few rows:")
                print(df.head(80))
                print("\nData types:")
                print(df.dtypes)
            else:
                print("Data content:", df)
                
        except Exception as e:
            print(f"Error reading file: {e}")
        
        print("\n" + "="*50)
else:
    print("No data directories found in", data_dir)
    