import pytest
import pandas as pd
import numpy as np
from data_preprocessing_module import DataPreprocessor
@pytest.fixture
def sample_historical_data():
    return pd.DataFrame({
        'Open': [1282.27, 1286.14, 1296.72],
        'High': [1291.16, 1297.16, 1298.16],
        'Low': [1277.85, 1284.68, 1280.93],
        'Close': [1284.68, 1293.96, 1283.73],
        'Volume': [9204156, np.nan, 10864584],
        'Dividends': [0.0, 0.0, 0.0],
        'Stock Splits': [0.0, 0.0, 0.0]
    }, index=pd.date_range('2023-12-27', periods=3, tz='Asia/Kolkata'))

@pytest.fixture
def sample_financial_data():
    # Create sample dates as columns
    dates = [pd.Timestamp('2024-03-31'), pd.Timestamp('2023-03-31'), 
            pd.Timestamp('2022-03-31'), pd.Timestamp('2021-03-31')]
    
    return {
        'balance_sheet': pd.DataFrame({
            dates[0]: [2.31e12, 3.46e12, 4.33e12],
            dates[1]: [2.80e12, 3.34e12, 3.85e12],
            dates[2]: [2.33e12, 2.82e12, 5.48e12],
            dates[3]: [2.37e12, 2.60e12, 5.55e12]
        }, index=['Net Debt', 'Total Debt', 'Tangible Book Value']),
        
        'income_statement': pd.DataFrame({
            dates[0]: [1.76e12],
            dates[1]: [1.55e12],
            dates[2]: [1.22e12],
            dates[3]: [8.93e11]
        }, index=['Normalized EBITDA']),
        
        'cash_flow': pd.DataFrame({
            dates[0]: [5.91e10],
            dates[1]: [-2.60e11],
            dates[2]: [1.05e11],
            dates[3]: [-7.89e11]
        }, index=['Free Cash Flow'])
    }

def test_historical_data_cleaning(sample_historical_data):
    preprocessor = DataPreprocessor()
    cleaned = preprocessor.clean_historical_data(sample_historical_data)
    
    assert cleaned is not None
    assert not cleaned['Volume'].isna().any()  # Missing values handled
    assert 'Returns' in cleaned.columns
    assert 'MA_50' in cleaned.columns
    assert 'Volatility_20D' in cleaned.columns
    assert 'Volume_MA_20' in cleaned.columns

def test_financial_statement_cleaning(sample_financial_data):
    preprocessor = DataPreprocessor()
    
    # Test balance sheet cleaning
    cleaned_bs = preprocessor.clean_financial_statement(
        sample_financial_data['balance_sheet'],
        'balance_sheet'
    )
    assert cleaned_bs is not None
    assert isinstance(cleaned_bs.index[0], pd.Timestamp)  # Dates should be in index after transpose
    
    # Test income statement cleaning
    cleaned_is = preprocessor.clean_financial_statement(
        sample_financial_data['income_statement'],
        'income_statement'
    )
    assert cleaned_is is not None
    assert not cleaned_is.isna().any().any()

def test_financial_ratios(sample_financial_data):
    preprocessor = DataPreprocessor()
    ratios = preprocessor.calculate_financial_ratios(sample_financial_data)
    
    assert ratios is not None
    assert 'Debt_Ratio' in ratios.columns
    assert 'EBITDA_Growth' in ratios.columns

def test_empty_dataframe_handling():
    preprocessor = DataPreprocessor()
    empty_df = pd.DataFrame()
    
    # Should handle empty DataFrame without errors
    result = preprocessor.clean_historical_data(empty_df)
    assert result.empty
    
    result = preprocessor.clean_financial_statement(empty_df, 'balance_sheet')
    assert result.empty

# def test_process_stock_data(sample_historical_data, sample_financial_data):
#     preprocessor = DataPreprocessor()
#     data_dict = {
#         'historical_1y_1d': sample_historical_data,
#         **sample_financial_data
#     }
    
#     processed = preprocessor.process_stock_data(data_dict)
#     assert processed is not None
#     assert 'historical_1y_1d' in processed
#     assert 'financial_ratios' in processed


def test_process_stock_data(sample_historical_data, sample_financial_data):
    preprocessor = DataPreprocessor()
    data_dict = {
        'historical_1y_1d': sample_historical_data,
        **sample_financial_data
    }
    
    processed = preprocessor.process_stock_data(data_dict)
    assert processed is not None
    assert 'historical_1y_1d' in processed
    
    # Check if financial_ratios are generated
    if 'financial_ratios' in processed:
        assert isinstance(processed['financial_ratios'], pd.DataFrame)
    else:
        print("Available keys in processed data:", processed.keys())
        print("\nBalance Sheet columns:", processed['balance_sheet'].index.tolist())
        print("\nIncome Statement columns:", processed['income_statement'].index.tolist())
        assert False, "financial_ratios not found in processed data"