import yfinance as yf
import pandas as pd
from typing import List, Optional, Union
import time
from datetime import datetime, timedelta
import logging
from ratelimit import limits, sleep_and_retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YahooFinanceCollector:
    """
    A class to collect financial data from Yahoo Finance with rate limiting and error handling.
    """
    
    # Rate limiting: 2000 calls per hour as per Yahoo Finance API guidelines
    CALLS = 2000
    RATE_LIMIT_PERIOD = 3600  # 1 hour in seconds
    
    def __init__(self):
        # Nifty 50 companies (you can extend this for BSE/NSE)
        self.nifty50_symbols = [
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HINDUNILVR.NS",
            "ICICIBANK.NS", "HDFC.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS"
            # Add more symbols as needed
        ]
    
    @sleep_and_retry
    @limits(calls=CALLS, period=RATE_LIMIT_PERIOD)
    def _fetch_data(self, symbol: str) -> Optional[yf.Ticker]:
        """
        Fetch data for a single symbol with rate limiting.
        """
        try:
            return yf.Ticker(symbol)
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def get_fundamental_data(self, symbol: str) -> dict:
        """
        Get fundamental data for a single stock.
        """
        try:
            ticker = self._fetch_data(symbol)
            print("Ticker: ", ticker)
            print("\n")
            if not ticker:
                return {}

            # Fetch various fundamental data
            info = ticker.info
            balance_sheet = ticker.balance_sheet
            income_stmt = ticker.income_stmt
            cash_flow = ticker.cashflow

            return {
                'info': info,
                'balance_sheet': balance_sheet,
                'income_statement': income_stmt,
                'cash_flow': cash_flow
            }
        except Exception as e:
            logger.error(f"Error getting fundamental data for {symbol}: {str(e)}")
            return {}

    def get_historical_data(self, symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """
        Get historical price data for a single stock.
        """
        try:
            ticker = self._fetch_data(symbol)
            if not ticker:
                return pd.DataFrame()

            hist_data = ticker.history(period=period, interval=interval)
            return hist_data
        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def get_nifty50_data(self, data_type: str = "fundamental") -> dict:
        """
        Get data for all Nifty 50 stocks.
        
        Args:
            data_type: Either "fundamental" or "historical"
        """
        results = {}
        for symbol in self.nifty50_symbols:
            logger.info(f"Fetching {data_type} data for {symbol}")
            
            try:
                if data_type == "fundamental":
                    results[symbol] = self.get_fundamental_data(symbol)
                else:
                    results[symbol] = self.get_historical_data(symbol)
                
                # Add delay to respect rate limits
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                results[symbol] = None
                
        return results

    def save_data(self, data: Union[dict, pd.DataFrame], filename: str):
        """
        Save data to file (CSV or Parquet).
        """
        try:
            if isinstance(data, pd.DataFrame):
                if filename.endswith('.parquet'):
                    data.to_parquet(filename)
                else:
                    data.to_csv(filename)
            else:
                pd.DataFrame(data).to_parquet(filename) if filename.endswith('.parquet') else pd.DataFrame(data).to_csv(filename)
            
            logger.info(f"Data successfully saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving data to {filename}: {str(e)}")
