import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        # Expected metrics for different data types
        self.expected_columns = {
            'historical': ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'],
            'balance_sheet': ['Net Debt', 'Total Debt', 'Tangible Book Value', 'Ordinary Shares Number'],
            'income_statement': ['Tax Rate For Calcs', 'Normalized EBITDA'],
            'cash_flow': ['Free Cash Flow', 'Repayment Of Debt', 'Issuance Of Debt', 'Capital Expenditure']
        }
        
        # Define acceptable ranges for different metrics
        self.value_ranges = {
            'price': (0, 1e6),  # Price shouldn't be negative or unreasonably high
            'volume': (0, 1e12),  # Volume should be positive
            'debt': (-1e15, 1e15),  # Range for debt values
            'ebitda': (0, 1e15)  # EBITDA should typically be positive
        }

    def validate_historical_data(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Validate historical price data with datetime index.
        """
        issues = {}
        
        # Check for proper datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            issues['invalid_index'] = "Index is not DatetimeIndex"
        
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

        # Check volume
        if 'Volume' in df.columns:
            invalid_volume = df[
                (df['Volume'] < self.value_ranges['volume'][0]) | 
                (df['Volume'] > self.value_ranges['volume'][1])
            ]
            if not invalid_volume.empty:
                issues['volume_anomalies'] = invalid_volume.index.tolist()

        return len(issues) == 0, issues

    def validate_fundamental_data(self, data_type: str, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Validate fundamental data (balance sheet, income statement, cash flow).
        Data structure: metrics in index, dates in columns
        """
        issues = {}
        
        # Check if columns are dates
        non_date_cols = [col for col in df.columns 
                        if not isinstance(col, pd.Timestamp)]
        if non_date_cols:
            issues['non_date_columns'] = non_date_cols
        
        # Check for required metrics in index
        if data_type in self.expected_columns:
            missing_metrics = set(self.expected_columns[data_type]) - set(df.index)
            if missing_metrics:
                issues['missing_metrics'] = list(missing_metrics)

        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            issues['null_values'] = null_counts[null_counts > 0].to_dict()

        # Data type specific validations
        if data_type == 'balance_sheet':
            # Check debt values
            for metric in ['Net Debt', 'Total Debt']:
                if metric in df.index:
                    invalid_debt = df.loc[metric][
                        (df.loc[metric] < self.value_ranges['debt'][0]) |
                        (df.loc[metric] > self.value_ranges['debt'][1])
                    ]
                    if not invalid_debt.empty:
                        issues[f'invalid_{metric.lower().replace(" ", "_")}'] = invalid_debt.index.tolist()
                        
        elif data_type == 'income_statement':
            # Check EBITDA
            if 'Normalized EBITDA' in df.index:
                invalid_ebitda = df.loc['Normalized EBITDA'][
                    (df.loc['Normalized EBITDA'] < self.value_ranges['ebitda'][0]) |
                    (df.loc['Normalized EBITDA'] > self.value_ranges['ebitda'][1])
                ]
                if not invalid_ebitda.empty:
                    issues['invalid_ebitda'] = invalid_ebitda.index.tolist()

        return len(issues) == 0, issues

    def validate_data_freshness(self, df: pd.DataFrame, max_age_days: int = 90) -> Tuple[bool, str]:
        """
        Check if the data is recent enough.
        Handles both DatetimeIndex and date columns.
        """
        if df.empty:
            return False, "Empty DataFrame"
            
        try:
            # Get latest date based on data structure
            if isinstance(df.index, pd.DatetimeIndex):
                latest_date = df.index.max()
                if latest_date.tz is not None:
                    latest_date = latest_date.tz_localize(None)  # Remove timezone
            
            
            elif all(isinstance(col, pd.Timestamp) for col in df.columns):
                latest_date = max(df.columns)
            else:
                return False, "No valid dates found"
            
            age = (pd.Timestamp.now() - latest_date).days
            if age > max_age_days:
                return False, f"Data is {age} days old (max allowed: {max_age_days})"
            return True, "Data is fresh"
            
        except Exception as e:
            return False, f"Error checking data freshness: {str(e)}"

    def run_all_validations(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Run all validations on a dictionary of DataFrames.
        Returns validation results for each data type.
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
                
                # Check data freshness
                fresh_status, fresh_msg = self.validate_data_freshness(df)
                results['freshness'] = {'status': fresh_status, 'message': fresh_msg}
                
            except Exception as e:
                results['status'] = False
                results['issues'] = {'error': str(e)}
                logger.error(f"Error validating {data_type}: {str(e)}")
            
            validation_results[data_type] = results
            
        return validation_results