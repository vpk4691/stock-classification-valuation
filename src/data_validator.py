# data_validator.py

###### example - to be modified later

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        # Expected columns for different data types
        self.expected_columns = {
            'historical': ['Open', 'High', 'Low', 'Close', 'Volume'],
            'balance_sheet': ['Total Assets', 'Total Liabilities'],
            'income_statement': ['Total Revenue', 'Net Income'],
            'cash_flow': ['Operating Cash Flow', 'Free Cash Flow']
        }
        
        # Define acceptable ranges for different metrics
        self.value_ranges = {
            'price': (0, 1e6),  # Price shouldn't be negative or unreasonably high
            'volume': (0, 1e12),  # Volume should be positive
            'percentage': (-100, 100)  # For percentage changes
        }

    def validate_historical_data(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Validate historical price data.
        """
        issues = {}
        
        # Check for required columns
        missing_cols = set(self.expected_columns['historical']) - set(df.columns)
        if missing_cols:
            issues['missing_columns'] = list(missing_cols)

        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            issues['null_values'] = null_counts[null_counts > 0].to_dict()

        # Check for price anomalies
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in df.columns:
                anomalies = df[
                    (df[col] < self.value_ranges['price'][0]) | 
                    (df[col] > self.value_ranges['price'][1])
                ]
                if not anomalies.empty:
                    issues[f'{col}_anomalies'] = anomalies.index.tolist()

        # Check price consistency
        if all(col in df.columns for col in ['High', 'Low', 'Open', 'Close']):
            inconsistent = df[
                (df['High'] < df['Low']) | 
                (df['High'] < df['Open']) | 
                (df['High'] < df['Close']) |
                (df['Low'] > df['Open']) | 
                (df['Low'] > df['Close'])
            ]
            if not inconsistent.empty:
                issues['price_inconsistencies'] = inconsistent.index.tolist()

        return len(issues) == 0, issues

    def validate_fundamental_data(self, data_type: str, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Validate fundamental data (balance sheet, income statement, cash flow).
        """
        issues = {}
        
        # Check for required columns
        if data_type in self.expected_columns:
            missing_cols = set(self.expected_columns[data_type]) - set(df.columns)
            if missing_cols:
                issues['missing_columns'] = list(missing_cols)

        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            issues['null_values'] = null_counts[null_counts > 0].to_dict()

        # Check for negative values where inappropriate
        if data_type == 'balance_sheet':
            negative_assets = df[df['Total Assets'] < 0]
            if not negative_assets.empty:
                issues['negative_assets'] = negative_assets.index.tolist()

        return len(issues) == 0, issues

    def validate_data_freshness(self, df: pd.DataFrame, max_age_days: int = 30) -> Tuple[bool, str]:
        """
        Check if the data is recent enough.
        """
        if df.empty:
            return False, "Empty DataFrame"
            
        try:
            latest_date = pd.to_datetime(df.index.max())
            age = (pd.Timestamp.now() - latest_date).days
            
            if age > max_age_days:
                return False, f"Data is {age} days old (max allowed: {max_age_days})"
            return True, "Data is fresh"
            
        except Exception as e:
            return False, f"Error checking data freshness: {str(e)}"

    def run_all_validations(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Run all validations on a dictionary of DataFrames.
        """
        validation_results = {}
        
        for data_type, df in data_dict.items():
            results = {}
            
            # Skip empty DataFrames
            if df.empty:
                results['status'] = False
                results['issues'] = {'error': 'Empty DataFrame'}
                validation_results[data_type] = results
                continue
                
            try:
                # Run appropriate validation based on data type
                if 'historical' in data_type.lower():
                    results['status'], results['issues'] = self.validate_historical_data(df)
                else:
                    results['status'], results['issues'] = self.validate_fundamental_data(data_type, df)
                
                # Check data freshness for all types
                fresh_status, fresh_msg = self.validate_data_freshness(df)
                results['freshness'] = {'status': fresh_status, 'message': fresh_msg}
                
            except Exception as e:
                results['status'] = False
                results['issues'] = {'error': str(e)}
            
            validation_results[data_type] = results
            
        return validation_results
