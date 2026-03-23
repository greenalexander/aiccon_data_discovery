import sdmx
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def download_dataset(provider, dataset_code, start_year="2020"):
    """
    Downloads an SDMX dataset and flattens it for research use.
    """
    print(f"📥 Connecting to {provider} to fetch {dataset_code}...")
    
    try:
        # 1. Initialize the client for the specific provider
        client = sdmx.Client(provider)
        
        # 2. Request the data with a time filter
        # Note: We use 'params' for the year to avoid downloading 40 years of data by accident
        params = {'startPeriod': str(start_year)}
        
        # Some providers require a specific 'key' (dimension filter). 
        # For a general download, we leave it empty (all dimensions).
        # key = {'GEO': 'IT'} # Example: filter for Italy only (uncomment if needed)
        msg = client.data(dataset_code, params=params)
        
        # 3. Convert the complex SDMX message to a Pandas DataFrame
        # SDMX returns a 'Series' with a MultiIndex. We need to reset it.
        data = sdmx.to_pandas(msg)
        
        if isinstance(data, pd.Series):
            df = data.to_frame(name='value').reset_index()
        else:
            df = data.reset_index()

        # 4. Save to CSV
        output_path = f"data/raw/data_{provider}_{dataset_code}.csv"
        df.to_csv(output_path, index=False)
        
        print(f"✅ Success! Downloaded {len(df)} rows.")
        print(f"📂 Saved to: {output_path}")
        return df

    except Exception as e:
        print(f"❌ Error downloading {dataset_code}: {e}")
        return None

if __name__ == "__main__":
    # Test with a common Eurostat dataset: 'irt_st_a' (Interest rates)
    # You can change this to an Istat code you found in your search!
    test_df = download_dataset("ESTAT", "irt_st_a", start_year="2022")
    
    if test_df is not None:
        print("\n--- Data Preview ---")
        print(test_df.head())