# api_wrapper.py

#### Example - to be refined later

class StockDataAPI:
    def __init__(self, api_key=None):
        self.collector = YahooFinanceCollector()
        self.api_key = api_key  # For future extensions with paid APIs
        self._cache = {}  # Simple caching
    
    def get_stock_data(self, symbol, data_type="all"):
        """Main interface method"""
        pass
    
    def bulk_download(self, symbols):
        """Batch processing method"""
        pass