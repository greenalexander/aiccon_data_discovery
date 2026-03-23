import sdmx
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def download_sdmx_data(agency_id, dataset_code, params=None):
    """Downloads an actual table from an SDMX provider."""
    print(f"📥 Downloading {dataset_code} from {agency_id}...")
    try:
        client = sdmx.Client(agency_id)
        # We request the data. params can include {'startPeriod': '2020'}
        msg = client.data(dataset_code, params=params)
        
        # Convert the complex SDMX object into a simple Pandas DataFrame
        df = sdmx.to_pandas(msg)
        
        # Handle MultiIndex (SDMX often returns nested headers)
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
            
        return df
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

if __name__ == "__main__":
    # Example: Let's try to download a small Eurostat dataset
    # 'irt_st_a' is the code for short-term interest rates
    data = download_sdmx_data("ESTAT", "irt_st_a", params={'startPeriod': '2022'})
    
    if data is not None:
        print("✅ Data retrieved!")
        print(data.head())
        data.to_csv("data/raw/downloaded_sample.csv", index=False)