# ALEX NEXT STEPS: 
1. NOW
(a) Translations for all files
(b) separate keyword filters? External to the other functions so I can change in one place only
(c) 1.7. Is it suspicious that it ave me 499 datasets? Maybe not
(d) discovery_engine and search_tool should search all sources from the yaml file. 


2. LATER
(b) discovery_engine/search_tool/downloader: Moving the files onto sharepoint
(c) Is it really downloading all these files to my computer? That's surely taking up space. Every time I search a key term, am I downloading hte entire database? 
(d) other sources? 
(e) regional datasets? CKAN for many but not all
8. Moving on to regional level datasets. many use CKAN but not all. 

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
