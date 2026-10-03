# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC Loads the two raw sources from the `raw_files` volume into Bronze Delta tables, **as-is** (no cleaning, no filtering).
# MAGIC
# MAGIC | Source | Raw file | Bronze table |
# MAGIC |---|---|---|
# MAGIC | SOPFEU wildfire origin points (Shapefile, 1972–2024) | `Feux_pt_ori_SHP.zip` | `wildfire_project.bronze.fires_raw` |
# MAGIC | ECCC daily weather, 3 stations × 2000–2025 (CSV) | `weather_2000_2025.zip` | `wildfire_project.bronze.weather_daily_raw` |
# MAGIC
# MAGIC Bronze rules followed here:
# MAGIC - All values stored as **strings**, exactly as in the source (type casting happens in Silver).
# MAGIC - Audit columns added: `_source_file`, `_ingested_at`.
# MAGIC - Only change to the data: weather column names are converted to snake_case, because Delta does not allow spaces/parentheses in column names.
# MAGIC - Tables are overwritten on each run, so the notebook can be re-run safely.

# COMMAND ----------

# MAGIC %pip install geopandas
# MAGIC %restart_python

# COMMAND ----------

import io
import re
import zipfile
from pathlib import Path

import geopandas as gpd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

CATALOG = "wildfire_project"
VOLUME_DIR = f"/Volumes/{CATALOG}/bronze/raw_files"
EXTRACT_DIR = f"{VOLUME_DIR}/extracted"

FIRES_ZIP = f"{VOLUME_DIR}/Feux_pt_ori_SHP.zip"
WEATHER_ZIP = f"{VOLUME_DIR}/weather_2000_2025.zip"

FIRES_TABLE = f"{CATALOG}.bronze.fires_raw"
WEATHER_TABLE = f"{CATALOG}.bronze.weather_daily_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unzip raw files into the volume
# MAGIC
# MAGIC `zipfile.extractall` cannot write directly to a volume on serverless, so each zip is read as bytes with Spark
# MAGIC and every member is uploaded through the Databricks SDK Files API.

# COMMAND ----------

w = WorkspaceClient()

for zip_path, target in [(FIRES_ZIP, f"{EXTRACT_DIR}/fires"), (WEATHER_ZIP, f"{EXTRACT_DIR}/weather")]:
    zip_bytes = spark.read.format("binaryFile").load(zip_path).select("content").first()["content"]
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            file_path = f"{target}/{info.filename}"
            dbutils.fs.mkdirs(file_path.rsplit("/", 1)[0])
            w.files.upload(file_path=file_path, contents=io.BytesIO(z.read(info.filename)), overwrite=True)
    print(f"{zip_path} -> {target}")

display(dbutils.fs.ls(f"{EXTRACT_DIR}/fires"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Wildfire origin points → `bronze.fires_raw`
# MAGIC
# MAGIC Spark cannot read Shapefiles natively, so the file is read with GeoPandas and converted to a Spark DataFrame.
# MAGIC The point geometry is kept as WKT text so nothing from the source is lost.

# COMMAND ----------

shp_path = next(Path(f"{EXTRACT_DIR}/fires").rglob("*.shp"))
gdf = gpd.read_file(shp_path)
print(f"Read {len(gdf):,} rows from {shp_path.name}, CRS = {gdf.crs}")

pdf = gdf.drop(columns="geometry").astype(str)
pdf["geometry_wkt"] = gdf.geometry.to_wkt()

fires_bronze = (
    spark.createDataFrame(pdf)
    .withColumn("_source_file", F.lit(shp_path.name))
    .withColumn("_ingested_at", F.current_timestamp())
)

fires_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FIRES_TABLE)
display(spark.table(FIRES_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Daily weather → `bronze.weather_daily_raw`
# MAGIC
# MAGIC 78 CSV files (one per station per year) are read in one pass. `_metadata.file_path` records which file each row came from.

# COMMAND ----------

def to_snake(name: str) -> str:
    """'Max Temp (°C)' -> 'max_temp_c', 'Longitude (x)' -> 'longitude_x'."""
    name = name.replace("﻿", "").lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

weather_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")   # keep everything as string in Bronze
    .option("encoding", "UTF-8")
    .csv(f"{EXTRACT_DIR}/weather/**/*.csv")
    .select("*", F.col("_metadata.file_path").alias("_source_file"))
)

weather_bronze = (
    weather_raw.toDF(*[to_snake(c) if c != "_source_file" else c for c in weather_raw.columns])
    .withColumn("_ingested_at", F.current_timestamp())
)

weather_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(WEATHER_TABLE)
display(spark.table(WEATHER_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation
# MAGIC
# MAGIC Expected: **44,471** fire records (1972–2024, unfiltered), **78** weather files, **9,497** days per station (2000-01-01 → 2025-12-31).

# COMMAND ----------

fires_count = spark.table(FIRES_TABLE).count()
print(f"fires_raw rows: {fires_count:,}")

display(
    spark.table(WEATHER_TABLE)
    .groupBy("station_name", "climate_id")
    .agg(
        F.countDistinct("_source_file").alias("files"),
        F.count("*").alias("days"),
        F.min("date_time").alias("first_day"),
        F.max("date_time").alias("last_day"),
    )
    .orderBy("station_name")
)

assert fires_count == 44471, f"Unexpected fire row count: {fires_count}"
assert spark.table(WEATHER_TABLE).select("_source_file").distinct().count() == 78, "Expected 78 weather files"
