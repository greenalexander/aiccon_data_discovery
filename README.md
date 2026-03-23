# ALEX NEXT STEPS: 

- discovery_engine.py, not working: Sistan, INPS Open Data, and WorldBank. 
- For the search_tool I need to allow multipel keywords (italian and english)
- downloader doesn't work for EU_Data? 

2. LATER
(a) clean up readme please
(b) discovery_engine/search_tool/downloader: Moving the files onto sharepoint
(c) Is it really downloading all these files to my computer? That's surely taking up space. Every time I search a key term, am I downloading hte entire database? 
(d) other sources? 
(e) regional datasets? CKAN for many but not all
Moving on to regional level datasets. many use CKAN but not all. 

REGIONAL_PORTALS = [
    {"region": "emilia-romagna", "url": "https://dati.emilia-romagna.it"},
    {"region": "lombardia",      "url": "https://dati.lombardia.it"},
    {"region": "toscana",        "url": "https://dati.toscana.it"},
    # ...
]

def fetch_ckan_portal(base_url, query="terzo settore"):
    url = f"{base_url}/api/3/action/package_search"
    params = {"q": query, "rows": 50}
    ...

- National:  ISTAT (SDMX) + RUNTS + ANPR
- Regional:  Loop over known CKAN portals (dati.REGIONE.it)
- Province:  ISTAT filtered by NUTS3 code
- Comuni:    ISTAT where available, otherwise manual/static


# AICCON Data Discovery Tool
**Mapping the Social Economy Landscape in Italy and Europe.**

## Overview
This tool is designed for **AICCON** to solve the "fragmented data" challenge. It identifies, maps, and catalogs datasets relevant to the social economy from major European and Italian providers (Istat, Eurostat, RUNTS, etc.) without requiring massive local storage.

## Features
- **The Map:** An automated metadata harvester that updates a central "library" of datasets.
- **Project Search:** Keyword-based discovery for new research tenders (e.g., "ethical finance", "cooperatives").
- **SharePoint Integration:** Metadata is stored in a shared Parquet format for PowerBI visualization.

## Tech Stack
- **Language:** Python 3.12+ (managed by `uv`)
- **Storage:** Apache Parquet (SharePoint backend)
- **Frontend:** PowerBI Dashboard
- **Key APIs:** Eurostat (SDMX), Istat, data.europa.eu

## Getting Started
1. **Prerequisites:** Install [uv](https://github.com/astral-sh/uv).
2. **Setup:** ```bash
   uv sync
   cp .env.example .env  # Add your SharePoint credentials here
3. **Usage:** 
Update the map: python src/discovery_engine.py
Search for a project: python src/search_tool.py --query "social impact"

## API Map: Strategic Data Sources
The following sources are prioritized for AICCON's research into the social economy and territorial fragility.

| ID | Source | API Type | Core Purpose for AICCON |
| :--- | :--- | :--- | :--- |
| **EU_DATA** | data.europa.eu | REST / SPARQL | "Harvester" of all European open data; best for discovery. |
| **ESTAT** | Eurostat | SDMX 3.0 | Cross-country comparisons of labor and social trends. |
| **ISTAT** | Istat (Italy) | SDMX 2.1 | Primary source for Italian non-profits (IstatData). |
| **RUNTS** | RUNTS (Italy) | Portal / REST | Tracking "Enti del Terzo Settore" (The Italian Registry). |
| **OECD** | OECD | SDMX | Global benchmarks for the Social Economy & Innovation. |
| **INPS** | INPS (Italy) | Open Data | Granular labor market data and cooperative employment. |
| **SISTAN** | Sistan (Italy) | SDMX | Data from the National Statistical System network. |
| **DG_EMP** | EU Social Affairs | REST | Specific EU policy indicators and funding trends. |
| **WORLD_BANK** | World Bank | REST | SGreat for social protection and financial inclusion (Findex) data. |


## License
[MIT/CC BY-NC] - Focused on open research for the social economy.




# AICCON Data Pipeline

Tools for discovering, searching, and downloading datasets from Italian and European open data sources.

---

## Folder structure

```
project/
├── config/
│   └── sources.yaml        ← list of data sources and keywords (edit here)
├── data/
│   ├── catalogue/          ← output of discovery_engine (Parquet, for PowerBI)
│   └── raw/                ← output of search_tool and downloader (CSV)
└── src/
    ├── discovery_engine.py ← monthly full catalogue refresh
    ├── search_tool.py      ← ad-hoc keyword search
    ├── downloader.py       ← download a specific dataset
    └── utils/
        └── core_utils.py   ← shared utilities (do not edit unless necessary)
```

---

## Setup

Install dependencies:
```bash
pip install pandas requests sdmx1 pyyaml python-dotenv pyarrow
```

Create a `.env` file in the project root for SharePoint credentials (if needed):
```
SHAREPOINT_EMAIL=your@email.com
SHAREPOINT_PASSWORD=yourpassword
SHAREPOINT_SITE_URL=https://yourorg.sharepoint.com/sites/yoursite
```

---

## Scripts

### 1. `discovery_engine.py` — monthly catalogue refresh

Searches all sources using the keyword list in `sources.yaml` and saves a master catalogue as a Parquet file for PowerBI.

```bash
python src/discovery_engine.py
```

Output: `data/catalogue/master_catalogue.parquet`

---

### 2. `search_tool.py` — ad-hoc search

Searches all (or selected) sources for a specific keyword and saves results as CSV.

```bash
# Search all sources
python src/search_tool.py --query "youth unemployment"

# Search only Istat and Eurostat
python src/search_tool.py --query "povertà" --sources ISTAT ESTAT

# Show more results in the terminal preview
python src/search_tool.py --query "social enterprise" --limit 20
```

Output: `data/raw/search_<query>_<date>.csv`

---

### 3. `downloader.py` — download a specific dataset

Downloads a dataset by its source ID and code. Use `search_tool.py` first to find the correct code.

```bash
# Download a Eurostat dataset
python src/downloader.py --source ESTAT --code lfsa_ergaed

# Download an Istat dataset filtered to Italy, from 2018
python src/downloader.py --source ISTAT --code DCIS_OCCUPAZIONE1 --start 2018 --geo IT

# Download an INPS dataset
python src/downloader.py --source INPS --code nome-dataset

# Download a World Bank indicator for Italy
python src/downloader.py --source WORLD_BANK --code SI.POV.GINI --geo ITA
```

Output: `data/raw/<SOURCE>_<code>.csv`

---

## Updating keywords

Open `config/sources.yaml` and edit the `keywords` section. Changes apply automatically to both `discovery_engine.py` and `search_tool.py` — no code changes needed.

```yaml
keywords:
  - "social economy"
  - "economia sociale"
  - "your new keyword"
  - "tua nuova parola chiave"
```

---

## Updating or adding sources

Open `config/sources.yaml` and add an entry under `sources`. The `type` field controls how the source is accessed:

| type        | description                              | example         |
|-------------|------------------------------------------|-----------------|
| `sdmx_2.1`  | Standard SDMX 2.1 API                   | ISTAT, OECD     |
| `sdmx_3.0`  | SDMX 3.0 API                            | Eurostat        |
| `ckan`      | CKAN open data portal API               | INPS            |
| `rest`      | Generic REST API (EU Hub, World Bank)   | EU_DATA         |
| `manual`    | No API — manual reference link only     | RUNTS           |

---

## Supported sources

| ID          | Name                          | Type      |
|-------------|-------------------------------|-----------|
| EU_DATA     | data.europa.eu                | REST      |
| ESTAT       | Eurostat                      | SDMX 3.0  |
| ISTAT       | Istat                         | SDMX 2.1  |
| SISTAN      | Sistan Hub                    | SDMX 2.1  |
| OECD        | OECD                          | SDMX 2.1  |
| DG_EMPL     | EU Social Affairs (DG EMPL)   | SDMX 2.1  |
| INPS        | INPS Open Data                | CKAN      |
| WORLD_BANK  | World Bank                    | REST      |
| RUNTS       | RUNTS (Terzo Settore registry)| Manual    |
