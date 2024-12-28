# import pandas as pd
# data = pd.read_parquet("reliance_data.parquet")
# print(data.head(10))


import pandas as pd
from glob import glob
import os

# Find latest data directory
data_dir = "../data"
timestamp_dirs = sorted(glob(os.path.join(data_dir, "*")))
latest_dir = timestamp_dirs[-1] if timestamp_dirs else None

if latest_dir:
    reliance_files = glob(os.path.join(latest_dir, "RELIANCE*"))
    for file in reliance_files:
        print(f"\nExamining file: {os.path.basename(file)}")
        df = pd.read_parquet(file)
        print("\nShape:", df.shape)
        print("\nIndex:", df.index.tolist()[:5])  # First 5 index values
        print("\nColumns:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        print("\n" + "="*50)