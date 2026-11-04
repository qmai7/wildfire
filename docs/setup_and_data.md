# Setup and Data Guide

How the repository, Databricks workspace and raw data are organized. See the [project proposal](../README.md) for scope.

Platform: Databricks Free Edition (Unity Catalog, Delta Lake, MLflow, Jobs) + Power BI.

## Repository layout

| Path | Contents |
|---|---|
| `notebooks/00_setup.sql` | Creates the catalog, schemas and `raw_files` volume (re-runnable) |
| `notebooks/01_bronze_ingestion.py` | Raw zips in the volume → `wildfire_project.bronze.*` Delta tables |
| `notebooks/02_silver_cleaning.py` | Bronze → cleaned `wildfire_project.silver.*` tables |
| `notebooks/03_gold_features.py` | Silver → fire–weather integration and model feature tables in `wildfire_project.gold.*` |
| `scripts/download_weather.py` | Downloads the ECCC weather CSVs and builds `weather_2000_2025.zip` (run locally) |

Notebooks are stored in Databricks source format (`.py`) so they diff cleanly in git and open as notebooks in Databricks Git folders.

## Data

Raw data is **not** committed. It is uploaded to the Unity Catalog volume `wildfire_project.bronze.raw_files`:

| File | Source |
|---|---|
| `Feux_pt_ori_SHP.zip` | SOPFEU / MRNF wildfire origin points, 1972–2024 — [Données Québec](https://www.donneesquebec.ca/) (CC-BY) |
| `weather_2000_2025.zip` | ECCC daily climate data, 2000–2025, one CSV per station per year — [climate.weather.gc.ca](https://climate.weather.gc.ca/historical_data/search_historic_data_e.html) |

Weather stations: 30 ECCC stations, listed with their Climate ID and Station ID in `scripts/download_weather.py`.
How they were chosen is in [B_dataset_summary.md](phase1/B_dataset_summary.md) (B.1, Station selection).

To rebuild the weather zip (about a minute, 780 requests), run this on your own machine, not in Databricks
(Free Edition limits outbound internet), then upload the zip to the volume:

```
python3 scripts/download_weather.py --out-dir weather --zip weather_2000_2025.zip
```

Weather download URL pattern (Station ID, not Climate ID):
`https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=<STATION_ID>&Year=<YEAR>&Month=1&Day=1&timeframe=2`

## Workflow

1. Pull latest `main` in your Databricks Git folder (or local clone).
2. Work on a branch, commit, push, open a pull request.
3. Merge to `main` after a teammate reviews.
