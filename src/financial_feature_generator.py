import pandas as pd
import numpy as np

class FinancialFeatureGenerator:
    def __init__(self):
        self.feature_names = []
    
    def preprocess_data(self, df):
        """Preprocess dataframe to clean column names and ensure correct types"""
        # Clean date columns - extract just the year
        year_cols = [str(col.year) for col in df.columns[:-1]]  # Get years from timestamps
        cleaned_cols = year_cols + [df.columns[-1]]  # Add back the 'Company' column
        df.columns = cleaned_cols
        return df
    
    def calculate_basic_ratios(self, df, year):
        """Calculate basic financial ratios for a given year"""
        features = {}
        
        # Debt Ratios
        features[f'debt_to_equity_{year}'] = (
            df.loc['Total Debt', year] / 
            df.loc['Total Equity Gross Minority Interest', year]
        )
        
        features[f'debt_to_assets_{year}'] = (
            df.loc['Total Debt', year] / 
            df.loc['Total Assets', year]
        )
        
        features[f'current_ratio_{year}'] = (
            df.loc['Current Assets', year] / 
            df.loc['Current Liabilities', year]
        )
        
        # Profitability Ratios
        features[f'roa_{year}'] = (
            df.loc['Net Income', year] / 
            df.loc['Total Assets', year]
        )
        
        features[f'roe_{year}'] = (
            df.loc['Net Income', year] / 
            df.loc['Total Equity Gross Minority Interest', year]
        )
        
        features[f'gross_margin_{year}'] = (
            df.loc['Gross Profit', year] / 
            df.loc['Total Revenue', year]
        )
        
        features[f'operating_margin_{year}'] = (
            df.loc['Operating Income', year] / 
            df.loc['Total Revenue', year]
        )
        
        return features
    
    def calculate_growth_metrics(self, df, current_year, previous_year):
        """Calculate growth metrics between two years"""
        features = {}
        
        # YoY Growth
        features[f'revenue_growth_{current_year}'] = (
            df.loc['Total Revenue', current_year] / 
            df.loc['Total Revenue', previous_year]
        ) - 1
        
        features[f'net_income_growth_{current_year}'] = (
            df.loc['Net Income', current_year] / 
            df.loc['Net Income', previous_year]
        ) - 1
        
        features[f'operating_income_growth_{current_year}'] = (
            df.loc['Operating Income', current_year] / 
            df.loc['Operating Income', previous_year]
        ) - 1
        
        return features
    
    def calculate_efficiency_metrics(self, df, year):
        """Calculate efficiency metrics"""
        features = {}
        
        features[f'asset_turnover_{year}'] = (
            df.loc['Total Revenue', year] / 
            df.loc['Total Assets', year]
        )
        
        features[f'working_capital_turnover_{year}'] = (
            df.loc['Total Revenue', year] / 
            df.loc['Working Capital', year]
        )
        
        return features
    
    def calculate_cash_flow_metrics(self, df, year):
        """Calculate cash flow related metrics"""
        features = {}
        
        features[f'fcf_to_revenue_{year}'] = (
            df.loc['Free Cash Flow', year] / 
            df.loc['Total Revenue', year]
        )
        
        features[f'operating_cf_ratio_{year}'] = (
            df.loc['Operating Cash Flow', year] / 
            df.loc['Current Liabilities', year]
        )
        
        features[f'capex_to_revenue_{year}'] = (
            abs(df.loc['Capital Expenditure', year]) / 
            df.loc['Total Revenue', year]
        )
        
        return features
    
    def calculate_greenblatt_metrics(self, df, year):
        """Calculate Joel Greenblatt's Magic Formula components"""
        features = {}
        
        # Return on Capital
        features[f'return_on_capital_{year}'] = (
            df.loc['Operating Income', year] / 
            (df.loc['Working Capital', year] + df.loc['Net Tangible Assets', year])
        )
        
        # Earnings Yield
        features[f'earnings_yield_{year}'] = (
            df.loc['Operating Income', year] / 
            df.loc['Total Capitalization', year]
        )
        
        return features
    
    def calculate_cagr(self, start_value, end_value, num_years):
        """Calculate Compound Annual Growth Rate"""
        return (end_value / start_value) ** (1/num_years) - 1
    
    def generate_all_features(self, df):
        """Generate all features for a company"""
        # Preprocess data
        df = self.preprocess_data(df)
        
        # Get years (excluding 'Company' column)
        years = [col for col in df.columns if col != 'Company']
        current_year = years[0]  # 2024
        previous_year = years[1]  # 2023
        
        # Initialize features dictionary
        features = {}
        company_name = df['Company'].iloc[0]
        features['Company'] = company_name
        
        # Calculate all metrics
        features.update(self.calculate_basic_ratios(df, current_year))
        features.update(self.calculate_growth_metrics(df, current_year, previous_year))
        features.update(self.calculate_efficiency_metrics(df, current_year))
        features.update(self.calculate_cash_flow_metrics(df, current_year))
        features.update(self.calculate_greenblatt_metrics(df, current_year))
        
        # Calculate 3-year CAGR
        if len(years) >= 3:
            three_years_ago = years[-1]  # 2021
            features['revenue_cagr_3yr'] = self.calculate_cagr(
                df.loc['Total Revenue', three_years_ago],
                df.loc['Total Revenue', current_year],
                3
            )
            features['net_income_cagr_3yr'] = self.calculate_cagr(
                df.loc['Net Income', three_years_ago],
                df.loc['Net Income', current_year],
                3
            )
        
        return pd.Series(features)

# Usage example:
# feature_generator = FinancialFeatureGenerator()
# features = feature_generator.generate_all_features(your_df)