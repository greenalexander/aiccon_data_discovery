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


## License
[MIT/CC BY-NC] - Focused on open research for the social economy.
