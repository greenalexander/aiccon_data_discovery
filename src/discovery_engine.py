# AICCON Discovery Engine
# -----------------------
# Fetches dataset metadata from all configured sources (sources.yaml) and saves
# a master catalogue as a Parquet file for use in the PowerBI dashboard.

# Run monthly:
#     python src/discovery_engine.py

# Output:
#     data/catalogue/master_catalogue.parquet


import sys
import os
import logging
import pandas as pd
import requests
import sdmx

# Allow imports from the src/utils folder regardless of working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.core_utils import (
    get_sources,
    build_keyword_pattern,
    matches_keywords,
    save_to_catalogue,
    ensure_directories,
    get_current_date,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence noisy third-party loggers
logging.getLogger("sdmx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# SOURCE-SPECIFIC FETCHERS
# Each returns a DataFrame with the standard catalogue columns, or an empty
# DataFrame on failure (with a printed warning).
# ---------------------------------------------------------------------------

STANDARD_COLUMNS = ["code", "title", "description", "provider", "last_updated", "keywords", "link", "source"]


def _empty_df():
    return pd.DataFrame(columns=STANDARD_COLUMNS)

def _extract_eu_keywords(kws):
    if not kws:
        return ""
    if isinstance(kws, dict):
        kws = kws.get('en') or kws.get('it') or []
    if not isinstance(kws, list):
        return ""
    extracted = []
    for item in kws[:5]:
        if isinstance(item, str):
            extracted.append(item)
        elif isinstance(item, dict):
            label = item.get('label') or item.get('name') or item.get('value') or str(item)
            extracted.append(label)
    return ", ".join(extracted)

def fetch_eu_hub(source, keyword_pattern):
    # Fetches from data.europa.eu REST API with pagination.
    # Sends each keyword as a query and deduplicates results.
    # Descriptions are available from this source.
    base_url = f"{source['base_url']}/search"
    all_results = []
    seen_ids = set()

    # Build query list from the keyword pattern's source terms
    from utils.core_utils import get_keywords
    queries = get_keywords() or ["social economy"]

    for query in queries:
        offset = 0
        page_size = 50
        max_results = 50
        while True:
            params = {"q": query, "limit": page_size, "page": offset // page_size}
            try:
                resp = requests.get(base_url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"[EU_HUB] Query '{query}' failed: {e}")
                break

            results = data.get('result', {}).get('results', [])
            if not results:
                break

            for d in results:
                dataset_id = d.get('id')
                if not dataset_id or dataset_id in seen_ids:
                    continue
                seen_ids.add(dataset_id)

                title_field = d.get('title', {})
                title = title_field.get('en') or title_field.get('it') or 'N/A'

                desc_field = d.get('description', {})
                description = desc_field.get('en') or desc_field.get('it') or None

                all_results.append({
                    "code": dataset_id,
                    "title": title,
                    "description": description,
                    "provider": d.get('catalogue', {}).get('title', source['name']),
                    "last_updated": d.get('modification_date', 'N/A'),
                    "keywords": _extract_eu_keywords(d.get('keywords', [])),
                    "link": f"https://data.europa.eu/data/datasets/{dataset_id}",
                    "source": source['id'],
                })

            # Stop if we've hit the cap or reached the last page
            if len(all_results) >= max_results or len(results) < page_size:
                break
            offset += page_size

    if not all_results:
        return _empty_df()

    df = pd.DataFrame(all_results)
    logger.info(f"[EU_HUB] {len(df)} unique datasets fetched.")
    return df


def fetch_sdmx(source):
    # Universal SDMX fetcher for sdmx_2.1 and sdmx_3.0 sources.
    # Extracts last_updated from dataflow annotations where available.
    # Descriptions are generally not available at the dataflow list level.
    agency = source.get('agency', source['id'])
    base_url = source.get('base_url')

    try:
        client = sdmx.Client(agency, url=base_url) if base_url else sdmx.Client(agency)
        msg = client.dataflow()
    except Exception as e:
        print(f"  ⚠️  Warning: Could not fetch from {source['name']} — {e}")
        return _empty_df()

    rows = []
    for fid, fobj in msg.dataflow.items():
        # Attempt to extract last_updated from annotations
        last_updated = "Check API"
        if hasattr(fobj, 'annotations'):
            for ann in fobj.annotations:
                if 'update' in str(ann.id).lower() or 'date' in str(ann.id).lower():
                    last_updated = str(ann.text)
                    break

        rows.append({
            "code": fid,
            "title": str(fobj.name),
            "description": None,  # Not available at dataflow list level
            "provider": source['name'],
            "last_updated": last_updated,
            "keywords": None,
            "link": f"SDMX:{agency}:{fid}",
            "source": source['id'],
        })

    df = pd.DataFrame(rows) if rows else _empty_df()
    logger.info(f"[{source['id']}] {len(df)} dataflows fetched.")
    return df


def fetch_ckan(source, keyword_pattern):
    # Fetches from CKAN-based portals (e.g. INPS).
    # Searches each keyword and deduplicates results.
    # Descriptions are available from CKAN.
    base_url = source['base_url']
    all_results = []
    seen_ids = set()

    from utils.core_utils import get_keywords
    queries = get_keywords() or ["social"]

    for query in queries:
        try:
            resp = requests.get(
                f"{base_url}/package_search",
                params={"q": query, "rows": 100},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"[{source['id']}] Query '{query}' failed: {e}")
            continue

        for pkg in data.get('result', {}).get('results', []):
            pkg_id = pkg.get('id')
            if not pkg_id or pkg_id in seen_ids:
                continue
            seen_ids.add(pkg_id)

            all_results.append({
                "code": pkg.get('name', pkg_id),
                "title": pkg.get('title', 'N/A'),
                "description": pkg.get('notes') or None,
                "provider": source['name'],
                "last_updated": pkg.get('metadata_modified', 'N/A'),
                "keywords": ", ".join([t.get('name', '') for t in pkg.get('tags', [])][:5]),
                "link": f"{base_url.replace('/api/3/action', '')}/dataset/{pkg.get('name', pkg_id)}",
                "source": source['id'],
            })

    if not all_results:
        return _empty_df()

    df = pd.DataFrame(all_results)
    logger.info(f"[{source['id']}] {len(df)} datasets fetched.")
    return df


def fetch_world_bank(source, keyword_pattern):
    # Fetches topic-level dataset indicators from the World Bank REST API.
    # Filters by topics relevant to social economy.
    # Descriptions are available.
    base_url = source['base_url']
    # Topic IDs most relevant to AICCON: 11=Poverty, 2=Aid, 6=Education, 13=Social Protection
    relevant_topics = ["11", "13", "2"]
    all_results = []
    seen_ids = set()

    for topic_id in relevant_topics:
        page = 1
        while True:
            try:
                resp = requests.get(
                    f"{base_url}/indicator",
                    params={"topic": topic_id, "format": "json", "per_page": 100, "page": page},
                    timeout=30
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as e:
                logger.warning(f"[WORLD_BANK] Topic {topic_id}, page {page} failed: {e}")
                break

            if not isinstance(payload, list) or len(payload) < 2:
                break

            meta = payload[0]
            indicators = payload[1] or []

            for ind in indicators:
                ind_id = ind.get('id')
                if not ind_id or ind_id in seen_ids:
                    continue
                seen_ids.add(ind_id)

                title = ind.get('name', 'N/A')
                # Apply keyword filter to avoid unrelated indicators
                if not matches_keywords(title, keyword_pattern):
                    continue

                all_results.append({
                    "code": ind_id,
                    "title": title,
                    "description": ind.get('sourceNote') or None,
                    "provider": source['name'],
                    "last_updated": "Check API",
                    "keywords": ind.get('topics', [{}])[0].get('value', '') if ind.get('topics') else '',
                    "link": f"https://data.worldbank.org/indicator/{ind_id}",
                    "source": source['id'],
                })

            if page >= min(meta.get('pages', 1), 10):
                break
            page += 1

    if not all_results:
        return _empty_df()

    df = pd.DataFrame(all_results)
    logger.info(f"[WORLD_BANK] {len(df)} indicators fetched.")
    return df

def fetch_manual(source):
    # Returns a single-row placeholder for manual/portal sources like RUNTS.
    return pd.DataFrame([{
        "code": source['id'],
        "title": source['name'],
        "description": source.get('note'),
        "provider": source['name'],
        "last_updated": "Live — check portal",
        "keywords": None,
        "link": source['base_url'],
        "source": source['id'],
    }])

# ---------------------------------------------------------------------------
# DISPATCHER
# Routes each source to the correct fetcher based on its 'type' field.
# ---------------------------------------------------------------------------

def fetch_source(source, keyword_pattern):
    # Dispatches a source config entry to the correct fetcher.
    source_type = source.get('type', '')
    source_id = source['id']

    print(f"  → Fetching {source['name']} ({source_type})...")

    if source_id == "EU_DATA" or source_type == "rest" and "europa" in source.get('base_url', ''):
        return fetch_eu_hub(source, keyword_pattern)
    elif source_type in ("sdmx_2.1", "sdmx_3.0", "sdmx"):
        return fetch_sdmx(source)
    elif source_type == "ckan":
        return fetch_ckan(source, keyword_pattern)
    elif source_id == "WORLD_BANK" or source_type == "rest" and "worldbank" in source.get('base_url', ''):
        return fetch_world_bank(source, keyword_pattern)
    elif source_type == "manual":
        return fetch_manual(source)
    else:
        print(f"  ⚠️  Warning: Unknown source type '{source_type}' for {source['name']} — skipping.")
        return _empty_df()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_discovery():
    print("\n🚀 AICCON Discovery Engine — starting full catalogue refresh...")
    print(f"   Date: {get_current_date()}\n")

    ensure_directories()
    sources = get_sources()
    keyword_pattern = build_keyword_pattern()

    if not sources:
        print("❌ No sources loaded from config. Check config/sources.yaml.")
        return

    print(f"📋 {len(sources)} sources loaded from config.\n")

    all_frames = []
    for source in sources:
        df = fetch_source(source, keyword_pattern)
        if df.empty:
            print(f"  ⚠️  No data returned from {source['name']}.")
        else:
            all_frames.append(df)

    if not all_frames:
        print("\n❌ No data collected from any source.")
        return

    master_df = pd.concat(all_frames, ignore_index=True)

    # Apply keyword filter to SDMX titles (EU Hub is pre-filtered by query)
    sdmx_mask = master_df['source'].isin(
        [s['id'] for s in sources if s.get('type', '') in ('sdmx_2.1', 'sdmx_3.0', 'sdmx')]
    )
    non_sdmx = master_df[~sdmx_mask]
    sdmx_filtered = master_df[sdmx_mask & master_df['title'].apply(
        lambda t: matches_keywords(t, keyword_pattern)
    )]
    final_df = pd.concat([non_sdmx, sdmx_filtered], ignore_index=True)

    # Add run metadata
    final_df['catalogue_date'] = get_current_date()

    save_to_catalogue(final_df)

    print(f"\n🏁 Discovery complete.")
    print(f"   Total datasets in catalogue: {len(final_df)}")
    print(f"   Breakdown by source:")
    for source_id, count in final_df['source'].value_counts().items():
        print(f"     {source_id}: {count}")
    print()
    print(final_df[['code', 'title', 'provider']].head(10).to_string(index=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    run_discovery()