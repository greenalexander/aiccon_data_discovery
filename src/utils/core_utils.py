import os
import pandas as pd
from datetime import datetime

def save_to_catalog(df, filename="master_map.parquet"):
    """Saves metadata to the shared SharePoint folder (or local for now)."""
    # For now, let's save locally in the project structure
    path = os.path.join("data", "catalog", filename)
    df.to_parquet(path, index=False)
    print(f"Catalog updated: {path} at {datetime.now()}")

def get_current_date():
    return datetime.now().strftime("%Y-%m-%d")