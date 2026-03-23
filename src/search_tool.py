import argparse
import pandas as pd
import sdmx
import logging
from datetime import datetime
from discovery_engine import fetch_data_europa

# Mute sdmx1 logs to keep terminal clean
logging.getLogger("sdmx").setLevel(logging.ERROR)

def live_search_sdmx(agency_id, query):
    """Pulls the current list from an agency and filters for the query."""
    print(f"📡 Querying {agency_id} live...")
    try:
        client = sdmx.Client(agency_id)
        msg = client.dataflow()
        
        results = []
        for df_id, df_obj in msg.dataflow.items():
            title = str(df_obj.name)
            if query.lower() in title.lower():
                results.append({
                    "code": df_id,
                    "title": title,
                    "last_updated": "Check API", 
                    "provider": agency_id,
                    "keywords": "Social Statistics",
                    "link": f"SDMX:{df_id}",
                    "source": "SDMX_LIVE"
                })
        return pd.DataFrame(results)
    except Exception as e:
        # Some agencies might be down or slow
        return pd.DataFrame()

def run_deep_search(query):
    print(f"\n🚀 STARTING LIVE GLOBAL SEARCH: '{query}'")
    
    # 1. LIVE Search: data.europa.eu (Covers thousands of portals)
    eu_results = fetch_data_europa(query=query)
    
    # 2. LIVE Search: Key Agencies (Istat, Eurostat, OECD)
    sdmx_results = []
    for agency in ['ISTAT', 'ESTAT', 'OECD']:
        sdmx_results.append(live_search_sdmx(agency, query))
    
    # Combine everything
    all_results = pd.concat([eu_results] + sdmx_results, ignore_index=True)
    
    if all_results.empty:
        print(f"\n❌ No datasets found across any source for '{query}'.")
    else:
        # Sort so the most relevant (by title length/match) appear first
        print(f"\n✨ SEARCH COMPLETE: Found {len(all_results)} datasets.")
        print("-" * 80)
        print(all_results[['provider', 'title', 'code']].head().to_string(index=False))
        print("-" * 80)
        print(f"... and {len(all_results) - 5} more. See full results in the saved CSV.")

        # Save specific project file
        filename = f"data/raw/project_deep_search_{query.replace(' ', '_')}.csv"
        all_results.to_csv(filename, index=False)
        print(f"💾 Results saved for your team: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AICCON Live Search Tool")
    parser.add_argument("--query", type=str, required=True, help="Specific keyword to find")
    args = parser.parse_args()
    
    run_deep_search(args.query)