# AICCON Dataset Downloader
# --------------------------
# Downloads a specific dataset from a configured source and saves it as CSV.

# Usage:
    # python src/downloader.py --source ESTAT --code lfsa_ergaed
    # python src/downloader.py --source ISTAT --code DCIS_OCCUPAZIONE1
    # python src/downloader.py --source INPS --code nome-dataset-inps
    # python src/downloader.py --source WORLD_BANK --code SI.POV.GINI
    # python src/downloader.py --source ESTAT --code lfsa_ergaed --start 2015
    # python src/downloader.py --source ISTAT --code DCIS_OCCUPAZIONE1 --geo IT

# Arguments:
    # --source    (required) Source ID from sources.yaml. E.g. ESTAT, ISTAT, INPS, WORLD_BANK
    # --code      (required) Dataset/indicator code to download.
    # --start     (optional) Start year for time filter. Default: 2015.
    # --geo       (optional) Geography filter (SDMX only). E.g. IT, EU27_2020. Default: none (all).


import sys
import os
import argparse
import logging
import pandas as pd
import requests
import sdmx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.core_utils import get_sources, ensure_directories

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("sdmx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


def _get_source_config(source_id):
    # Looks up a source config entry by ID.
    sources = get_sources()
    for s in sources:
        if s['id'] == source_id:
            return s
    return None


def _save(df, source_id, dataset_code):
    # Saves a downloaded dataset to data/raw/ as CSV.
    ensure_directories()
    safe_code = dataset_code.replace('/', '_').replace('\\', '_')
    output_path = os.path.join("data", "raw", f"{source_id}_{safe_code}.csv")
    df.to_csv(output_path, index=False)
    print(f"📂 Saved to: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# SOURCE-SPECIFIC DOWNLOADERS
# ---------------------------------------------------------------------------

def download_sdmx(source, dataset_code, start_year="2015", geo_filter=None):
    # Downloads an SDMX dataset and flattens it to a DataFrame.
    # Supports optional start year and geography filter.

    agency = source.get('agency', source['id'])
    base_url = source.get('base_url')

    print(f"📡 Connecting to {source['name']} (agency: {agency})...")

    try:
        client = sdmx.Client(agency, url=base_url) if base_url else sdmx.Client(agency)
    except Exception as e:
        print(f"❌ Could not initialise SDMX client for {source['name']}: {e}")
        return None

    params = {'startPeriod': str(start_year)}
    key = {}
    if geo_filter:
        key['GEO'] = geo_filter
        print(f"   Geography filter applied: {geo_filter}")

    try:
        msg = client.data(dataset_code, key=key or None, params=params)
    except Exception as e:
        print(f"❌ Error fetching dataset '{dataset_code}' from {source['name']}: {e}")
        print("   Tip: Check that the code is correct using search_tool.py first.")
        return None

    try:
        data = sdmx.to_pandas(msg)
        df = data.to_frame(name='value').reset_index() if isinstance(data, pd.Series) else data.reset_index()
    except Exception as e:
        print(f"❌ Error converting SDMX data to DataFrame: {e}")
        return None

    print(f"✅ Downloaded {len(df)} rows from {source['name']}.")
    return df


def download_ckan(source, dataset_code):
    # Downloads a dataset from a CKAN portal by package name/ID.
    # Fetches the first CSV resource available.

    base_url = source['base_url']
    print(f"📡 Connecting to {source['name']}...")

    try:
        resp = requests.get(
            f"{base_url}/package_show",
            params={"id": dataset_code},
            timeout=30
        )
        resp.raise_for_status()
        pkg = resp.json().get('result', {})
    except Exception as e:
        print(f"❌ Could not fetch package metadata for '{dataset_code}': {e}")
        return None

    # Find the first CSV resource
    csv_resource = None
    for resource in pkg.get('resources', []):
        if resource.get('format', '').upper() == 'CSV':
            csv_resource = resource
            break

    if not csv_resource:
        print(f"❌ No CSV resource found in dataset '{dataset_code}'.")
        print(f"   Available formats: {[r.get('format') for r in pkg.get('resources', [])]}")
        return None

    download_url = csv_resource.get('url')
    print(f"   Found CSV resource: {csv_resource.get('name', 'N/A')}")
    print(f"   Downloading from: {download_url}")

    try:
        df = pd.read_csv(download_url)
    except Exception as e:
        print(f"❌ Error reading CSV from {download_url}: {e}")
        return None

    print(f"✅ Downloaded {len(df)} rows from {source['name']}.")
    return df


def download_world_bank(source, indicator_code, start_year="2015", geo_filter=None):
    # Downloads World Bank indicator data.
    # If geo_filter is provided (e.g. 'IT'), fetches only that country.
    # Otherwise fetches all countries.
    base_url = source['base_url']
    country = geo_filter if geo_filter else "all"
    print(f"📡 Connecting to {source['name']} (indicator: {indicator_code}, country: {country})...")

    all_rows = []
    page = 1
    while True:
        try:
            resp = requests.get(
                f"{base_url}/country/{country}/indicator/{indicator_code}",
                params={
                    "format": "json",
                    "per_page": 1000,
                    "page": page,
                    "date": f"{start_year}:2100"
                },
                timeout=30
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            print(f"❌ Error fetching World Bank indicator '{indicator_code}': {e}")
            return None

        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            break

        meta = payload[0]
        records = payload[1]

        for rec in records:
            all_rows.append({
                "country_id": rec.get('countryiso3code'),
                "country_name": rec.get('country', {}).get('value'),
                "indicator_id": rec.get('indicator', {}).get('id'),
                "indicator_name": rec.get('indicator', {}).get('value'),
                "year": rec.get('date'),
                "value": rec.get('value'),
            })

        if page >= meta.get('pages', 1):
            break
        page += 1

    if not all_rows:
        print(f"❌ No data returned for indicator '{indicator_code}'.")
        return None

    df = pd.DataFrame(all_rows)
    print(f"✅ Downloaded {len(df)} rows from {source['name']}.")
    return df


# ---------------------------------------------------------------------------
# MAIN DISPATCHER
# ---------------------------------------------------------------------------

def download_dataset(source_id, dataset_code, start_year="2015", geo_filter=None):
    # Main entry point. Looks up the source config and routes to the correct downloader.

    source = _get_source_config(source_id)
    if not source:
        print(f"❌ Source '{source_id}' not found in config/sources.yaml.")
        print(f"   Available sources: {[s['id'] for s in get_sources()]}")
        return None

    source_type = source.get('type', '')

    if source_type == "manual":
        print(f"❌ '{source_id}' is a manual reference source and cannot be downloaded programmatically.")
        print(f"   Visit: {source.get('base_url')}")
        return None
    elif source_type in ("sdmx_2.1", "sdmx_3.0", "sdmx"):
        df = download_sdmx(source, dataset_code, start_year=start_year, geo_filter=geo_filter)
    elif source_type == "ckan":
        df = download_ckan(source, dataset_code)
    elif source_id == "WORLD_BANK" or (source_type == "rest" and "worldbank" in source.get('base_url', '')):
        df = download_world_bank(source, dataset_code, start_year=start_year, geo_filter=geo_filter)
    else:
        print(f"❌ No downloader implemented for source type '{source_type}'.")
        return None

    if df is not None and not df.empty:
        _save(df, source_id, dataset_code)
        print("\n--- Preview (first 5 rows) ---")
        print(df.head().to_string(index=False))

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AICCON Dataset Downloader — download a specific dataset from a configured source."
    )
    parser.add_argument(
        "--source", type=str, required=True,
        help="Source ID from sources.yaml. E.g. ESTAT, ISTAT, INPS, WORLD_BANK"
    )
    parser.add_argument(
        "--code", type=str, required=True,
        help="Dataset or indicator code to download."
    )
    parser.add_argument(
        "--start", type=str, default="2015",
        help="Start year for time filter (default: 2015). Applies to SDMX and World Bank."
    )
    parser.add_argument(
        "--geo", type=str, default=None,
        help="Optional geography filter. E.g. IT, EU27_2020 (SDMX) or ITA (World Bank)."
    )
    args = parser.parse_args()

    download_dataset(
        source_id=args.source,
        dataset_code=args.code,
        start_year=args.start,
        geo_filter=args.geo,
    )