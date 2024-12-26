import pandas as pd
data = pd.read_parquet("reliance_data.parquet")
print(data.head())