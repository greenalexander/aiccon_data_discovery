# AICCON Search Tool
# ------------------
# Searches all configured sources for a specific keyword query and saves results
# as a CSV for ad-hoc research needs.

# Usage:
    # python src/search_tool.py --query "youth employment"
    # python src/search_tool.py --query "povertà" --sources ISTAT ESTAT
    # python src/search_tool.py --query "social enterprise" --limit 50

# Arguments:
    # --query     (required) The keyword or phrase to search for.
    # --sources   (optional) Space-separated list of source IDs to search.
    #                        Defaults to all sources. Example: ISTAT ESTAT OECD
    # --limit     (optional) Max results to display in terminal preview. Default: 10.


import sys
import os
import argparse
import logging
import pandas as pd
import requests
import sdmx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.discovery_engine import _extract_eu_keywords
from utils.core_utils import (
    get_sources,
    build_keyword_pattern,
    matches_keywords,
    save_search_results,
    ensure_directories,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("sdmx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

STANDARD_COLUMNS = ["code", "title", "description", "provider", "last_updated", "keywords", "link", "source", "relevance_score"]


def _empty_df():
    return pd.DataFrame(columns=STANDARD_COLUMNS)


def score_relevance(title, query):
    # Simple relevance score based on how much of the query appears in the title.
    # Returns an integer: higher = more relevant.
    if not title:
        return 0
    title_lower = title.lower()
    query_lower = query.lower()
    score = 0
    # Exact phrase match
    if query_lower in title_lower:
        score += 10
    # Individual word matches
    for word in query_lower.split():
        if len(word) > 2 and word in title_lower:
            score += 2
    return score


# ---------------------------------------------------------------------------
# SOURCE-SPECIFIC SEARCH FUNCTIONS
# ---------------------------------------------------------------------------

def search_eu_hub(source, query):
    # Searches data.europa.eu with pagination for the specific query.
    base_url = f"{source['base_url']}/search"
    all_results = []
    seen_ids = set()
    offset = 0
    page_size = 100

    while True:
        params = {"q": query, "limit": page_size, "page": offset // page_size}
        try:
            resp = requests.get(base_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  Warning: EU Hub search failed for '{query}': {e}")
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
                "relevance_score": score_relevance(title, query),
            })

        if len(results) < page_size:
            break
        offset += page_size

    return pd.DataFrame(all_results) if all_results else _empty_df()


def search_sdmx(source, query):
    # Fetches all dataflows from an SDMX agency and filters by query.
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
        title = str(fobj.name)
        # Filter: title must contain the query (bilingual — checked as-is)
        if query.lower() not in title.lower():
            continue

        last_updated = "Check API"
        if hasattr(fobj, 'annotations'):
            for ann in fobj.annotations:
                if 'update' in str(ann.id).lower() or 'date' in str(ann.id).lower():
                    last_updated = str(ann.text)
                    break

        rows.append({
            "code": fid,
            "title": title,
            "description": None,
            "provider": source['name'],
            "last_updated": last_updated,
            "keywords": None,
            "link": f"SDMX:{agency}:{fid}",
            "source": source['id'],
            "relevance_score": score_relevance(title, query),
        })

    return pd.DataFrame(rows) if rows else _empty_df()


def search_ckan(source, query):
    # Searches a CKAN portal with the specific query.
    base_url = source['base_url']
    try:
        resp = requests.get(
            f"{base_url}/package_search",
            params={"q": query, "rows": 100},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ⚠️  Warning: CKAN search failed for {source['name']}: {e}")
        return _empty_df()

    rows = []
    for pkg in data.get('result', {}).get('results', []):
        title = pkg.get('title', 'N/A')
        rows.append({
            "code": pkg.get('name', pkg.get('id')),
            "title": title,
            "description": pkg.get('notes') or None,
            "provider": source['name'],
            "last_updated": pkg.get('metadata_modified', 'N/A'),
            "keywords": ", ".join([t.get('name', '') for t in pkg.get('tags', [])][:5]),
            "link": f"{base_url.replace('/api/3/action', '')}/dataset/{pkg.get('name', '')}",
            "source": source['id'],
            "relevance_score": score_relevance(title, query),
        })

    return pd.DataFrame(rows) if rows else _empty_df()


def search_world_bank(source, query):
    # Searches World Bank indicators by query string.
    base_url = source['base_url']
    try:
        resp = requests.get(
            f"{base_url}/indicator",
            params={"format": "json", "per_page": 100, "mrv": 1, "source": 2},
            timeout=30
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        print(f"  ⚠️  Warning: World Bank search failed: {e}")
        return _empty_df()

    if not isinstance(payload, list) or len(payload) < 2:
        return _empty_df()

    rows = []
    for ind in payload[1] or []:
        title = ind.get('name', 'N/A')
        if query.lower() not in title.lower():
            continue
        rows.append({
            "code": ind.get('id'),
            "title": title,
            "description": ind.get('sourceNote') or None,
            "provider": source['name'],
            "last_updated": "Check API",
            "keywords": ind.get('topics', [{}])[0].get('value', '') if ind.get('topics') else '',
            "link": f"https://data.worldbank.org/indicator/{ind.get('id')}",
            "source": source['id'],
            "relevance_score": score_relevance(title, query),
        })

    return pd.DataFrame(rows) if rows else _empty_df()


# ---------------------------------------------------------------------------
# DISPATCHER
# ---------------------------------------------------------------------------

def search_source(source, query):
    # Routes a source to the correct search function.
    source_type = source.get('type', '')
    source_id = source['id']

    if source_type == "manual":
        return _empty_df()  # Manual sources are not searchable
    elif source_id == "EU_DATA" or (source_type == "rest" and "europa" in source.get('base_url', '')):
        return search_eu_hub(source, query)
    elif source_type in ("sdmx_2.1", "sdmx_3.0", "sdmx"):
        return search_sdmx(source, query)
    elif source_type == "ckan":
        return search_ckan(source, query)
    elif source_id == "WORLD_BANK" or (source_type == "rest" and "worldbank" in source.get('base_url', '')):
        return search_world_bank(source, query)
    else:
        print(f"  ⚠️  Unknown source type '{source_type}' for {source['name']} — skipping.")
        return _empty_df()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_search(query, source_filter=None, preview_limit=10):
    print(f"\n🔍 AICCON Search Tool — query: '{query}'")

    ensure_directories()
    all_sources = get_sources()

    # Apply source filter if provided
    if source_filter:
        sources = [s for s in all_sources if s['id'] in source_filter]
        if not sources:
            print(f"❌ No sources matched the filter: {source_filter}")
            return
        print(f"   Searching {len(sources)} source(s): {[s['id'] for s in sources]}\n")
    else:
        sources = all_sources
        print(f"   Searching all {len(sources)} configured sources.\n")

    all_frames = []
    for source in sources:
        print(f"  → Searching {source['name']}...")
        df = search_source(source, query)
        if not df.empty:
            all_frames.append(df)
            print(f"     Found {len(df)} result(s).")
        else:
            print(f"     No results.")

    if not all_frames:
        print(f"\n❌ No datasets found for '{query}' across any source.")
        return

    results = pd.concat(all_frames, ignore_index=True)

    # Sort by relevance score descending
    results = results.sort_values('relevance_score', ascending=False).reset_index(drop=True)

    print(f"\n✨ Search complete: {len(results)} datasets found across {results['source'].nunique()} source(s).")
    print("-" * 80)
    print(results[['provider', 'title', 'code']].head(preview_limit).to_string(index=False))
    if len(results) > preview_limit:
        print(f"... and {len(results) - preview_limit} more — see saved CSV.")
    print("-" * 80)

    saved_path = save_search_results(results, query)
    print(f"\n💾 Full results saved to: {saved_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AICCON Live Search Tool — search all configured data sources."
    )
    parser.add_argument(
        "--query", type=str, required=True,
        help="Keyword or phrase to search for. Supports English and Italian."
    )
    parser.add_argument(
        "--sources", type=str, nargs="+", default=None,
        help="Optional: space-separated source IDs to restrict search. E.g. --sources ISTAT ESTAT OECD"
    )
    parser.add_argument(
        "--limit", type=int, default=10,
        help="Number of results to preview in the terminal (default: 10)."
    )
    args = parser.parse_args()

    run_search(query=args.query, source_filter=args.sources, preview_limit=args.limit)