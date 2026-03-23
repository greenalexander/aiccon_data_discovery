import pandas as pd
import requests
import sdmx
import logging
from datetime import datetime
from utils.core_utils import load_config, save_to_catalogue

logger = logging.getLogger(__name__)

def fetch_data_europa(query="social economy"):
    """Search the massive EU Hub for relevant keywords."""
    url = "https://data.europa.eu/api/hub/search/search"
    params = {"q": query, "limit": 50}
    try:
        res = requests.get(url, params=params).json()
        return pd.DataFrame([{
            "code": d.get('id'),
            "title": d.get('title', {}).get('en', d.get('title', {}).get('it', 'N/A')),
            "provider": d.get('catalogue', {}).get('title'),
            "last_updated": d.get('modification_date', 'N/A'),
            "keywords": ", ".join(d.get('keywords', {}).get('en', [])[:3]),
            "link": f"https://data.europa.eu/data/datasets/{d.get('id')}",
            "source": "EU_HUB"
        } for d in res.get('result', {}).get('results', [])])
    except: return pd.DataFrame()

def fetch_sdmx_agency(agency_id, provider_url=None):
    """Universal SDMX fetcher for ISTAT, ESTAT, OECD, WB, and DG_EMPL."""
    try:
        # If a custom URL is provided (like for DG_EMPL), we use it
        client = sdmx.Client(agency_id, url=provider_url) if provider_url else sdmx.Client(agency_id)
        msg = client.dataflow()
        return pd.DataFrame([{
            "code": fid,
            "title": str(fobj.name),
            "provider": agency_id,
            "last_updated": "Check API", 
            "keywords": "Social Statistics",
            "link": f"SDMX:{fid}",
            "source": "SDMX"
        } for fid, fobj in msg.dataflow.items()])
    except: return pd.DataFrame()

def run_discovery():
    logger.info("Starting Full Discovery Load...")
    all_data = []

    # 1. EU Hub (The Broad Search)
    all_data.append(fetch_data_europa())

    # 2. Standard SDMX Sources
    for agency in ['ISTAT', 'ESTAT', 'OECD']:
        all_data.append(fetch_sdmx_agency(agency))

    # 3. Custom/Special Sources
    # DG EMPL (Social Affairs)
    dg_empl_url = "https://webgate.ec.europa.eu/empl/redisstat/api/dissemination/sdmx/2.1"
    all_data.append(fetch_sdmx_agency("EMPL", provider_url=dg_empl_url))
    
    # World Bank
    all_data.append(fetch_sdmx_agency("WB_WDI"))

    # 4. RUNTS / INPS Placeholder (Manual Links for Dashboard)
    all_data.append(pd.DataFrame([{
        "code": "RUNTS_REGISTRY", "title": "Registro Unico Nazionale Terzo Settore",
        "provider": "Ministero del Lavoro", "last_updated": "Live",
        "keywords": "ETS, Non-profit, Italy", "link": "https://servizi.lavoro.gov.it/runts/it-it/", "source": "MANUAL"
    }]))

    # Combine, Filter, and Save
    master_df = pd.concat(all_data, ignore_index=True)
    
    # Filtering for AICCON relevance
    keywords = ['social', 'coop', 'third sector', 'terzo settore', 'poverty', 'fragility']
    pattern = '|'.join(keywords)
    final_df = master_df[master_df['title'].str.contains(pattern, case=False, na=False) | (master_df['source'] == "EU_HUB")]

    save_to_catalogue(final_df)
    print(f"Success! Catalogue saved with {len(final_df)} datasets.")
    print(final_df[['code', 'title', 'provider']].head()) # terminal preview

if __name__ == "__main__":
    # Force output to terminal immediately
    print("🚀 Script started...") 
    
    logging.basicConfig(level=logging.ERROR)
    
    # Check if config exists before even trying
    import os
    if not os.path.exists("config/sources.yaml"):
        print("❌ ERROR: config/sources.yaml not found! Check your folder structure.")
    else:
        print("✅ Config found. Starting discovery...")
        run_discovery()
        print("🏁 Script finished.")