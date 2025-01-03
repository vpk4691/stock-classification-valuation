import pandas as pd
import numpy as np
from typing import Dict, List

class FinancialFeatureGenerator:
    def __init__(self):
        # Market cap categorization thresholds (you can adjust these)
        self.MARKET_CAP_THRESHOLDS = {
            'high': 500000000000,  # 500B
            'medium': 50000000000,  # 50B
        }
        
    def preprocess_data(self, df):
        """Preprocess dataframe to clean column names and ensure correct types"""
        year_cols = [str(col.year) for col in df.columns[:-1]]
        cleaned_cols = year_cols + [df.columns[-1]]
        df.columns = cleaned_cols
        return df
    
    def calculate_historical_average(self, df, metric_func, years, prefix=''):
        """Calculate both current and 3-year average for a given metric"""
        features = {}
        current_year = years[0]
        
        # Calculate current year metric
        current_metrics = metric_func(df, current_year)
        features.update({f"{k}_current" if prefix else k: v 
                        for k, v in current_metrics.items()})
        
        # Calculate 3-year average
        if len(years) >= 3:
            three_year_metrics = {}
            for year in years[:3]:  # Only take last 3 years
                year_metrics = metric_func(df, year)
                for k, v in year_metrics.items():
                    if k not in three_year_metrics:
                        three_year_metrics[k] = []
                    three_year_metrics[k].append(v)
            
            # Calculate averages
            for k, values in three_year_metrics.items():
                features[f"{k}_3yr_avg" if prefix else f"{k}_avg"] = np.mean(values)
        
        return features
    
    def calculate_basic_ratios(self, df, year):
        """Calculate basic financial ratios"""
        features = {}
        
        # Profitability Ratios
        features['net_profit_margin'] = (
            df.loc['Net Income', year] / df.loc['Total Revenue', year]
        )
        
        features['cost_revenue_ratio'] = (
            df.loc['Cost Of Revenue', year] / df.loc['Total Revenue', year]
        )
        
        # Liquidity Ratios
        features['current_ratio'] = (
            df.loc['Current Assets', year] / df.loc['Current Liabilities', year]
        )
        
        features['quick_ratio'] = (
            (df.loc['Current Assets', year]) / df.loc['Current Liabilities', year]
        )
        
        features['cash_ratio'] = (
            df.loc['End Cash Position', year] / df.loc['Current Liabilities', year]
        )
        
        # Efficiency Ratios
        features['asset_turnover'] = (
            df.loc['Total Revenue', year] / df.loc['Total Assets', year]
        )
        
        # Leverage Ratios
        features['debt_to_equity'] = (
            df.loc['Total Debt', year] / df.loc['Total Equity Gross Minority Interest', year]
        )
        
        return features

    def calculate_valuation_metrics(self, df, year):
        """Calculate valuation metrics"""
        features = {}
        
        # Greenblatt's Magic Formula Components
        features['roc'] = (
            df.loc['Operating Income', year] /
            (df.loc['Working Capital', year] + df.loc['Net Tangible Assets', year])
        )
        
        # Buffett's Owner Earnings
        features['owner_earnings'] = (
            df.loc['Operating Cash Flow', year] + df.loc['Capital Expenditure', year]
        )
        
        # FCF Yield components
        features['fcf_yield'] = (
            df.loc['Free Cash Flow', year] / df.loc['Total Capitalization', year]
        )
        
        return features

    def calculate_altman_z_score(self, df, year):
        """Calculate Altman Z-Score"""
        working_capital = df.loc['Working Capital', year]
        total_assets = df.loc['Total Assets', year]
        retained_earnings = df.loc['Retained Earnings', year]
        ebit = df.loc['Operating Income', year]
        total_equity = df.loc['Total Equity Gross Minority Interest', year]
        total_liabilities = df.loc['Total Debt', year]
        sales = df.loc['Total Revenue', year]
        
        z_score = (
            1.2 * (working_capital / total_assets) +
            1.4 * (retained_earnings / total_assets) +
            3.3 * (ebit / total_assets) +
            0.6 * (total_equity / total_liabilities) +
            0.999 * (sales / total_assets)
        )
        
        return {'altman_z_score': z_score}

    def calculate_graham_metrics(self, df, year):
        """Calculate Graham's metrics"""
        features = {}
        
        # Graham's Net-Net Working Capital
        features['graham_nnwc'] = (
            df.loc['Current Assets', year] -
            df.loc['Total Debt', year]
        )
        
        # Graham Number = √(22.5 * EPS * BVPS)
        eps = df.loc['Diluted EPS', year]
        book_value_per_share = df.loc['Net Tangible Assets', year]  # Approximate
        features['graham_number'] = np.sqrt(22.5 * eps * book_value_per_share)
        
        return features

    def generate_all_features(self, df):
        """Generate all features for a company"""
        df = self.preprocess_data(df)
        years = [col for col in df.columns if col != 'Company']
        
        features = {}
        company_name = df['Company'].iloc[0]
        features['Company'] = company_name
        
        # Calculate current and historical averages for all metrics
        features.update(self.calculate_historical_average(df, self.calculate_basic_ratios, years))
        features.update(self.calculate_historical_average(df, self.calculate_valuation_metrics, years))
        
        # Add Altman Z-Score
        current_year = years[0]
        features.update(self.calculate_altman_z_score(df, current_year))
        
        # Add Graham metrics
        features.update(self.calculate_graham_metrics(df, current_year))
        
        # Market cap categorization
        market_cap = df.loc['Total Capitalization', current_year]
        if market_cap >= self.MARKET_CAP_THRESHOLDS['high']:
            features['market_cap_category'] = 'high'
        elif market_cap >= self.MARKET_CAP_THRESHOLDS['medium']:
            features['market_cap_category'] = 'medium'
        else:
            features['market_cap_category'] = 'low'
        
        return pd.Series(features)

    def process_multiple_companies(self, df_dict):
        """Process multiple companies and return a DataFrame with all features"""
        all_features = []
        
        for company, df in df_dict.items():
            try:
                features = self.generate_all_features(df)
                all_features.append(features)
            except Exception as e:
                print(f"Error processing company {company}: {str(e)}")
                continue
        
        return pd.DataFrame(all_features)