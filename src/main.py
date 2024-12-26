from yahoo_finance_collector import YahooFinanceCollector

def main():
    # Initialize the collector
    collector = YahooFinanceCollector()
    
    # Test with a single stock (Reliance)
    print("Fetching Reliance data...")
    reliance_data = collector.get_historical_data("RELIANCE.NS")
    collector.save_data(reliance_data, "reliance_data.parquet")
    
    print("Data collection complete!")

if __name__ == "__main__":
    main()