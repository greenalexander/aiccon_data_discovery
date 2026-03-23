import os
import yaml
import logging
from datetime import datetime
from dotenv import load_dotenv

# Initialize logging to see what's happening during the run
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path="config/sources.yaml"):
    """Loads the API source configuration from the YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config.get('sources', [])
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        return []

def get_current_date():
    return datetime.now().strftime("%Y-%m-%d")

def save_to_catalogue(df, filename="master_map.parquet"):
    """Saves metadata to the shared SharePoint folder (or local for now)."""
    # For now, let's save locally in the project structure
    path = os.path.join("data", "catalogue", filename)
    df.to_parquet(path, index=False)
    print(f"Catalogue updated: {path} at {datetime.now()}")

def get_sharepoint_credentials():
    """
    Loads SharePoint credentials from the .env file.
    Returns a dictionary of secrets.
    """
    load_dotenv() # This looks for a .env file in the project root
    
    creds = {
        "email": os.getenv("SHAREPOINT_EMAIL"),
        "password": os.getenv("SHAREPOINT_PASSWORD"),
        "site_url": os.getenv("SHAREPOINT_SITE_URL")
    }
    
    # Validation: Check if any are missing
    missing = [k for k, v in creds.items() if not v]
    if missing:
        logger.warning(f"Missing SharePoint credentials in .env: {', '.join(missing)}")
        
    return creds

def ensure_directories():
    """Creates necessary data folders if they don't exist."""
    directories = ["data/raw", "data/catalogue", "notebooks"]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

if __name__ == "__main__":
    # Test block to ensure utility functions work
    ensure_directories()
    sources = load_config()
    print(f"Loaded {len(sources)} sources from YAML.")
    print(f"First source: {sources[0]['name']}")