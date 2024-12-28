import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from data_validator import DataValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        self.validator = DataValidator()
    
    def clean_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean historical price data
        - Handles missing values
        - Calculates additional technical indicators
        """
        if df.empty:
            return df
            
        df = df.copy()
        
        # Handle missing values
        df['Volume'] = df['Volume'].fillna(0)
        df = df.ffill()
        
        # Remove duplicate indices
        df = df[~df.index.duplicated(keep='first')]
        
        # Calculate returns
        df['Returns'] = df['Close'].pct_change()
        
        # Add technical indicators
        # Moving averages
        df['MA_50'] = df['Close'].rolling(window=50).mean()
        df['MA_200'] = df['Close'].rolling(window=200).mean()
        
        # Volatility (20-day)
        df['Volatility_20D'] = df['Returns'].rolling(window=20).std()
        
        # Trading volume indicators
        df['Volume_MA_20'] = df['Volume'].rolling(window=20).mean()
        
        return df
    
    def clean_financial_statement(self, df: pd.DataFrame, statement_type: str) -> pd.DataFrame:
        """
        Clean financial statements (balance sheet, income stmt, cash flow)
        - Transpose dates from columns to index if needed
        - Handle missing values
        - Convert values to proper numeric format
        """
        if df.empty:
            return df
            
        df = df.copy()
        
        # Transpose if dates are in columns
        if all(isinstance(col, pd.Timestamp) for col in df.columns):
            df = df.transpose()
        
        # Convert string values to numeric
        df = df.apply(pd.to_numeric, errors='coerce')
        
        # Handle missing values based on statement type
        if statement_type == 'balance_sheet':
            df = df.ffill()  # Use previous period's values
        elif statement_type in ['income_statement', 'cash_flow']:
            df = df.fillna(0)  # Use 0 for missing flow values
        
        return df
    
    def clean_info_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean company info data"""
        if df.empty:
            return df
            
        df = df.copy()
        
        # Convert numeric columns to proper type
        numeric_columns = [
            'marketCap', 'trailingPE', 'forwardPE', 'priceToBook',
            'returnOnEquity', 'returnOnAssets', 'totalRevenue'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def calculate_financial_ratios(self, data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Calculate financial ratios based on available data"""
        try:
            # Initialize DataFrame with dates as index
            dates = None
            for df in [data_dict.get('balance_sheet'), data_dict.get('income_statement')]:
                if df is not None and not df.empty:
                    dates = df.columns
                    break
                    
            if dates is None:
                return pd.DataFrame()
                
            ratios = pd.DataFrame(index=dates)
            
            bs = data_dict.get('balance_sheet')
            is_stmt = data_dict.get('income_statement')
            cf = data_dict.get('cash_flow')
            
            if bs is not None:
                if all(x in bs.index for x in ['Total Debt', 'Net Debt']):
                    ratios['Debt_Ratio'] = bs.loc['Total Debt'] / bs.loc['Net Debt']
                    
                if 'Tangible Book Value' in bs.index:
                    ratios['Tangible_Book_Value_Growth'] = bs.loc['Tangible Book Value'].pct_change()
            
            if is_stmt is not None and 'Normalized EBITDA' in is_stmt.index:
                ratios['EBITDA_Growth'] = is_stmt.loc['Normalized EBITDA'].pct_change()
                
            if cf is not None and 'Free Cash Flow' in cf.index:
                ratios['FCF_Growth'] = cf.loc['Free Cash Flow'].pct_change()
                
            return ratios
                
        except Exception as e:
            logger.error(f"Error calculating ratios: {str(e)}")
            return pd.DataFrame()
        
    
    def process_stock_data(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        
        """
        Main processing function for all stock data
        Returns dictionary with processed datasets
        """
        processed_data = {}
        
        try:
            # Process historical data
            for key in data_dict:
                if 'historical' in key:
                    processed_data[key] = self.clean_historical_data(data_dict[key])
            
            # Process financial statements - ensure dates stay as columns
            if 'balance_sheet' in data_dict:
                df = self.clean_financial_statement(data_dict['balance_sheet'], 'balance_sheet')
                if df.index.dtype == 'datetime64[ns]':  # If dates moved to index
                    df = df.transpose()  # Move them back to columns
                processed_data['balance_sheet'] = df
                
            if 'income_statement' in data_dict:
                df = self.clean_financial_statement(data_dict['income_statement'], 'income_statement')
                if df.index.dtype == 'datetime64[ns]':
                    df = df.transpose()
                processed_data['income_statement'] = df
                
            if 'cash_flow' in data_dict:
                df = self.clean_financial_statement(data_dict['cash_flow'], 'cash_flow')
                if df.index.dtype == 'datetime64[ns]':
                    df = df.transpose()
                processed_data['cash_flow'] = df
            
            # Calculate financial ratios only if we have the required data
            ratios = self.calculate_financial_ratios(processed_data)
            if not ratios.empty:
                processed_data['financial_ratios'] = ratios
            
            # Run validations
            validation_results = self.validator.run_all_validations(processed_data)
            processed_data['validation_results'] = validation_results
            
        except Exception as e:
            logger.error(f"Error in data processing: {str(e)}")
        
        return processed_data


    def combine_all_metrics(self, data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Combine historical data with financial metrics
        Returns a DataFrame with all relevant metrics aligned by date
        """
        try:
            # Start with the daily historical data if available
            base_df = None
            for key in data_dict:
                if 'historical' in key and '1d' in key:
                    base_df = data_dict[key].copy()
                    break
            
            if base_df is None:
                logger.error("No daily historical data found")
                return pd.DataFrame()
            
            # Add financial ratios
            if 'financial_ratios' in data_dict:
                ratios = data_dict['financial_ratios']
                # Forward fill ratios to align with daily data
                for col in ratios.columns:
                    base_df[f'ratio_{col}'] = ratios[col].reindex(base_df.index, method='ffill')
            
            return base_df
            
        except Exception as e:
            logger.error(f"Error combining metrics: {str(e)}")
            return pd.DataFrame()