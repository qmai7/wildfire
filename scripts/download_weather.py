"""Download ECCC daily weather for the project's 30 stations and build weather_2000_2025.zip.

Run locally (Free Edition notebooks have restricted outbound internet), then upload the zip to
the Unity Catalog volume wildfire_project.bronze.raw_files and run 01_bronze_ingestion.

    python3 scripts/download_weather.py --out-dir weather --zip weather_2000_2025.zip

Station selection (see docs/phase1/B_dataset_summary.md, B.1):
  1. ECCC Station Inventory: Québec stations whose daily record spans 2000–2024 (150 stations).
  2. Kept stations with April–October data on >= 85% of days for max temperature and precipitation,
     and >= 70% for precipitation in every sampled year (2002, 2007, 2012, 2017, 2022): 81 stations.
  3. Greedy pick: each step adds the station that brings the most 2000–2024 fires within 50 km.
     The first 30 are listed below, in pick order.
"""
import argparse
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

URL = ("https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
       "?format=csv&stationID={station_id}&Year={year}&Month=1&Day=1&timeframe=2")
YEARS = range(2000, 2026)

# (climate_id, station_id, ECCC station name)
STATIONS = [
    ("703GDKB", 10868, "ST-JOVITE"),
    ("7038975", 5642, "WRIGHT"),
    ("7016960", 5262, "ST CHARLES DE MANDEVILLE"),
    ("7026465", 5440, "RICHMOND"),
    ("701S001", 26892, "QUEBEC/JEAN LESAGE INTL"),
    ("7085106", 5988, "MONT BRUN"),
    ("7024745", 10761, "MCTAVISH"),
    ("7065639", 10872, "NORMANDIN"),
    ("7066820", 5936, "ST AMBROISE"),
    ("7034365", 5604, "LUSKVILLE"),
    ("701LEEH", 5203, "LAC AUX SABLES"),
    ("7080468", 5977, "BARRAGE TEMISCAMINGUE"),
    ("707DBD4", 10875, "LA TUQUE"),
    ("7044288", 5701, "LES BUISSONS"),
    ("7054096", 26893, "LA POCATIERE"),
    ("7028754", 26777, "BEAUCEVILLE"),
    ("701Q009", 29714, "LEMIEUX"),
    ("7077570", 5969, "ST MICHEL DES SAINTS"),
    ("7022579", 10815, "FRELIGHSBURG"),
    ("7056616", 8539, "RIVIERE-DU-LOUP"),
    ("7033939", 5599, "LA MACAZA"),
    ("7024320", 5398, "LINGWICK"),
    ("7056068", 27194, "POINTE-AU-PERE (INRS)"),
    ("7035666", 5619, "NOTRE DAME DE LA PAIX"),
    ("7086716", 10849, "ROUYN"),
    ("7034482", 5606, "MANIWAKI AIRPORT"),
    ("7028700", 5564, "VERCHERES"),
    ("709CEE9", 6005, "LAC BERRY"),
    ("7075800", 5966, "PARENT"),
    ("7055422", 10898, "NEW CARLISLE 1"),
]


def file_name(name, station_id, year):
    """'POINTE-AU-PERE (INRS)', 27194, 2000 -> 'POINTE-AU-PERE_INRS_27194_2000.csv'."""
    safe = "".join(ch if ch.isalnum() or ch == "-" else "_" for ch in name)
    safe = "_".join(part for part in safe.split("_") if part)
    return f"{safe}_{station_id}_{year}.csv"


def download(job, out_dir, retries=3):
    _, station_id, name, year = job
    target = out_dir / file_name(name, station_id, year)
    if target.exists() and target.stat().st_size > 0:
        return target
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(URL.format(station_id=station_id, year=year), timeout=60) as resp:
                body = resp.read()
            if not body.lstrip(b"\xef\xbb\xbf").startswith(b'"Longitude'):
                raise ValueError("response is not an ECCC daily CSV")
            target.write_bytes(body)
            return target
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out-dir", default="weather", help="folder for the CSV files")
    parser.add_argument("--zip", default="weather_2000_2025.zip", help="zip archive to create")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = [(cid, sid, name, year) for cid, sid, name in STATIONS for year in YEARS]
    with ThreadPoolExecutor(max_workers=4) as pool:
        files = list(pool.map(lambda j: download(j, out_dir), jobs))

    with zipfile.ZipFile(args.zip, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(files):
            z.write(f, arcname=f.name)
    print(f"{len(files)} files ({len(STATIONS)} stations x {len(YEARS)} years) -> {args.zip}")


if __name__ == "__main__":
    main()
