import os
import yaml
import logging
import re
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path="config/sources.yaml"):
    # Loads the full configuration (sources + keywords) from the YAML file.
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        return {}


def get_sources(config_path="config/sources.yaml"):
    # Returns only the list of sources from the YAML config.
    config = load_config(config_path)
    return config.get('sources', [])


def get_keywords(config_path="config/sources.yaml"):
    # Returns the keyword list from the YAML config.
    config = load_config(config_path)
    return config.get('keywords', [])


def build_keyword_pattern(config_path="config/sources.yaml"):
    # Builds a compiled regex pattern from the YAML keyword list.
    # Used to filter dataset titles consistently across all scripts.
    # Example match: 'Social enterprise statistics' or 'Impresa sociale 2023'
    keywords = get_keywords(config_path)
    if not keywords:
        logger.warning("No keywords found in config. No title filtering will be applied.")
        return None
    escaped = [re.escape(kw) for kw in keywords]
    pattern = re.compile('|'.join(escaped), flags=re.IGNORECASE)
    return pattern


def matches_keywords(text, pattern):
    # Returns True if the given text matches the keyword pattern.
    # Safe to call with None text or None pattern.
    if not text or pattern is None:
        return False
    return bool(pattern.search(str(text)))


def get_current_date():
    return datetime.now().strftime("%Y-%m-%d")


def save_to_catalogue(df, filename="master_catalogue.parquet"):
    # Saves the metadata catalogue as a Parquet file.
    # Creates the output directory if it does not exist.
    path = os.path.join("data", "catalogue", filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Catalogue saved: {path} ({len(df)} records) at {datetime.now()}")
    print(f"✅ Catalogue saved: {path} ({len(df)} records)")


def save_search_results(df, query):
    # Saves search results as a CSV file named after the query.
    # Creates the output directory if it does not exist.
    safe_query = query.replace(' ', '_').lower()
    filename = f"search_{safe_query}_{datetime.now().strftime('%Y%m%d')}.csv"
    path = os.path.join("data", "raw", filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Search results saved: {path} ({len(df)} records)")
    print(f"✅ Results saved: {path} ({len(df)} records)")
    return path


def get_sharepoint_credentials():
    # Loads SharePoint credentials from the .env file.
    # Returns a dictionary of secrets.
    load_dotenv()
    creds = {
        "email": os.getenv("SHAREPOINT_EMAIL"),
        "password": os.getenv("SHAREPOINT_PASSWORD"),
        "site_url": os.getenv("SHAREPOINT_SITE_URL")
    }
    missing = [k for k, v in creds.items() if not v]
    if missing:
        logger.warning(f"Missing SharePoint credentials in .env: {', '.join(missing)}")
    return creds

def ensure_directories():
    # Creates necessary data folders if they don't exist.
    directories = ["data/raw", "data/catalogue"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


if __name__ == "__main__":
    ensure_directories()
    sources = get_sources()
    keywords = get_keywords()
    print(f"Loaded {len(sources)} sources and {len(keywords)} keywords from config.")
    print(f"Sources: {[s['name'] for s in sources]}")