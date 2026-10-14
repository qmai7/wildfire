# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Silver: Data Cleaning
# MAGIC
# MAGIC Cleans each source **separately** (no integration here — the spatial/temporal join and all feature engineering happen in Gold).
# MAGIC
# MAGIC | Input (Bronze) | Output (Silver) | Key |
# MAGIC |---|---|---|
# MAGIC | `bronze.fires_raw` | `silver.fires` | `fire_id` |
# MAGIC | `bronze.weather_daily_raw` | `silver.weather_daily` | (`climate_id`, `date`) |
# MAGIC | `bronze.weather_daily_raw` | `silver.weather_stations` | `climate_id` |
# MAGIC | — | `silver.cleaning_log` | every cleaning action and how many rows it affected |
# MAGIC
# MAGIC Order of operations, for each dataset:
# MAGIC 1. **Type casting & standardization** — strings → dates/numbers, disguised null markers → real nulls, snake_case names.
# MAGIC 2. **Null profiling** — where values are missing and why.
# MAGIC 3. **Outlier detection** — on *observed* values only, before anything is imputed. Impossible values are set to null; plausible extremes are flagged and kept.
# MAGIC 4. **Null handling** — deletion, FD-based derivation, interpolation, group-mean imputation, depending on the column and the gap length.
# MAGIC 5. **Write** Silver tables with primary/foreign key constraints.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "wildfire_project"
BRONZE_FIRES = f"{CATALOG}.bronze.fires_raw"
BRONZE_WEATHER = f"{CATALOG}.bronze.weather_daily_raw"
SILVER = f"{CATALOG}.silver"

YEAR_MIN, YEAR_MAX = 2000, 2024   # project scope for fires

# Null handling thresholds for weather (days of consecutive missing values)
INTERP_MAX_GAP = 3        # gaps of 1–3 days: linear interpolation (temperatures only)
GROUP_MEAN_MAX_GAP = 30   # gaps of up to 30 days: station × month mean; longer gaps are left null

# Every cleaning action is recorded here and written to silver.cleaning_log at the end
cleaning_log = []

def log(dataset, step, column, rows, action):
    cleaning_log.append((dataset, step, column, int(rows), action))
    print(f"[{dataset}] {step} | {column}: {int(rows):,} rows -> {action}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1 — Wildfires
# MAGIC ## 1.1 Type casting and standardization
# MAGIC
# MAGIC Bronze stores everything as strings, and GeoPandas writes missing values as the text `nan`, `None` or `NaT`.
# MAGIC These **disguised nulls** would not be caught by `IS NULL`, so they are counted first and converted to real nulls.
# MAGIC
# MAGIC **Column names and categorical values are standardized to English** (e.g. `ANNEE` → `year`, `SUP_HA` → `area_ha`;
# MAGIC `Foudre` → `Lightning`, `Humaine` → `Human`, `Nordique` → `Nordic`) for consistency with the weather dataset.

# COMMAND ----------

NULL_MARKERS = ["", "nan", "NaN", "None", "NaT", "null"]
FIRE_COLS = ["CLE", "ANNEE", "NOFEU", "SECTION", "CAUSE", "DATE_DEBUT", "DATE_RAPPO", "DATE_ETEIN", "SUP_HA", "LATITUDE", "LONGITUDE"]

fires_bronze = spark.table(BRONZE_FIRES)

display(
    fires_bronze.select(
        *[F.sum(F.trim(F.col(c)).isin(NULL_MARKERS).cast("int")).alias(c) for c in FIRE_COLS]
    )
)

# COMMAND ----------

def clean_str(c):
    """Trim, and turn disguised null markers into real nulls (None)."""
    s = F.trim(F.col(c))
    return F.when(
        s.isin(NULL_MARKERS), None).otherwise(s)

def parse_date(c):
    """Shapefile dates may arrive as 'yyyyMMdd' or 'yyyy-MM-dd' depending on the reader; accept both."""
    s = clean_str(c)
    return (
        F.when(s.rlike(r"^\d{8}$"), F.to_date(s, "yyyyMMdd"))
         .when(s.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(s, 1, 10), "yyyy-MM-dd"))
    )

fires_typed = (
    fires_bronze
    .select(
        clean_str("CLE").alias("fire_id"),
        F.expr("try_cast(trim(ANNEE) AS INT)").alias("year"),
        F.expr("try_cast(trim(NOFEU) AS INT)").alias("fire_number"),
        F.when(clean_str("SECTION") == "Nordique", "Nordic")
         .otherwise(clean_str("SECTION")).alias("section"),
        F.when(clean_str("CAUSE") == "Foudre", "Lightning")
         .when(clean_str("CAUSE") == "Humaine", "Human")
         .otherwise(clean_str("CAUSE")).alias("cause"),
        parse_date("DATE_DEBUT").alias("start_date"),
        parse_date("DATE_RAPPO").alias("report_date"),
        parse_date("DATE_ETEIN").alias("extinction_date"),
        F.expr("try_cast(trim(SUP_HA) AS DOUBLE)").alias("area_ha"),
        F.expr("try_cast(trim(LATITUDE) AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(trim(LONGITUDE) AS DOUBLE)").alias("longitude"),
    )
)

# Dropped: OBJECTID (not an identifier — 0 for most rows) and geometry_wkt (redundant: fully determined by latitude/longitude).
n_all = fires_typed.count()
fires_typed = fires_typed.filter(F.col("year").between(YEAR_MIN, YEAR_MAX))
n_scope = fires_typed.count()
log("fires", "filter", "year", n_all - n_scope, f"removed: outside project scope {YEAR_MIN}-{YEAR_MAX}")
log("fires", "translate", "cause", n_scope, "Foudre -> Lightning, Humaine -> Human")
log("fires", "translate", "section", n_scope, "Nordique -> Nordic")
log("fires", "drop column", "OBJECTID, geometry_wkt", n_scope, "dropped: non-identifier / redundant with latitude+longitude")
print(f"{n_scope:,} fires in scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Null profiling
# MAGIC
# MAGIC A value is counted as missing if it is null after standardization, or if it failed to parse (text present but not a valid date/number).

# COMMAND ----------

null_counts = fires_typed.select(
    *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in fires_typed.columns]
)
display(null_counts)

for c, v in null_counts.first().asDict().items():
    if v:
        log("fires", "null profile", c, v, "missing after type casting")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Disguised nulls: `area_ha = 0`
# MAGIC
# MAGIC About 40% of fires report a burned area of exactly 0 ha. Before accepting that, check whether 0 is used as a placeholder for "unknown":
# MAGIC - If it were a placeholder, the share of zeros would typically jump in some years (a change in recording practice).
# MAGIC - In 24 of 25 years (2000–2022, 2024), areas are recorded at 0.1 ha precision, so a fire smaller than 0.05 ha rounds to 0.
# MAGIC - In 2023, areas use full floating-point precision (values like 0.0002 ha appear), so zeros in that year are unambiguously real
# MAGIC
# MAGIC The 2023 non-increment values span the full range — from `0.006` ha all the way up to `1,020,559` ha — all at full float precision. So 2023 wasn't just more precise for small fires; its entire recording method changed (likely satellite-derived areas rather than manual estimates rounded to 0.1 ha).

# COMMAND ----------

display(
    fires_typed.groupBy("year")
    .agg(F.count("*").alias("fires"), 
         F.sum((F.col("area_ha") == 0).cast("int")).alias("zero_area"))
    .withColumn("zero_share_pct", F.round(100 * F.col("zero_area") / F.col("fires"), 1))
    .orderBy("year")
)

display(
    fires_typed.filter(F.col("area_ha") < 0.5)
    .groupBy(F.round("area_ha", 2).alias("area_ha")).count().orderBy("area_ha")
)

# COMMAND ----------

log("fires", "disguised null check", "area_ha", fires_typed.filter(F.col("area_ha") == 0).count(),
    "kept: 0 ha is a real value, not a missing-value code (24 years at 0.1 ha precision: < 0.05 ha rounds to 0; 2023 full float precision: 0 is genuine)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Outlier detection
# MAGIC
# MAGIC ### Domain (validity) rules
# MAGIC Values that are impossible rather than unusual.

# COMMAND ----------

# Approximate bounding box of Québec
# Any fire outside this rectangle is either a data-entry error or a fire from another province that was mislabeled.
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 44.9, 62.6, -79.8, -57.0

rules = {
    # Fire was reported before it started — impossible chronology
    "report_before_start":     F.col("report_date") < F.col("start_date"),
    # Fire was extinguished before it started — impossible chronology
    "extinction_before_start": F.col("extinction_date") < F.col("start_date"),
    # The start date's year doesn't match the recorded fire year 
    "start_year_mismatch":     F.year("start_date") != F.col("year"),
    # Coordinate is outside Québec — wrong location or wrong dataset
    "outside_quebec":          ~F.col("latitude").between(LAT_MIN, LAT_MAX) | ~F.col("longitude").between(LON_MIN, LON_MAX),
    # Negative burned area
    "negative_area":           F.col("area_ha") < 0,
}
# Counting violations
violations = fires_typed.select(
    *[F.sum(F.coalesce(cond, F.lit(False)).cast("int")).alias(name) 
      for name, cond in rules.items()])

display(violations)

for name, v in violations.first().asDict().items():
    log("fires", "validity rule", name, v, "flagged is_valid = false" if v else "no violations")

# Adding is_valid flag to the dataset in case any rows that violate any domain rule are flagged invalid
is_valid = ~F.greatest(*[F.coalesce(cond, F.lit(False)) for cond in rules.values()])
fires_checked = fires_typed.withColumn("is_valid", is_valid)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statistical outliers on burned area
# MAGIC
# MAGIC Burned area is extremely right-skewed (median 0.1 ha, maximum over 1 million ha), so three methods are compared:
# MAGIC 1. **IQR on raw values** — the fence collapses to a few hectares and flags thousands of ordinary fires.
# MAGIC - Why it fails here: with 40% zeros, Q1 is 0 and Q3 is only a few hectares. The fence lands at a few hectares and flags thousands of ordinary fires. It’s included as the baseline that shows the problem.
# MAGIC 2. **IQR on log10(area)** — the standard fix for skewed data; flags only the genuinely large fires.
# MAGIC - Idea: apply the same Tukey fence after taking log10 of the area, then convert the fence back with 10 ** fence.
# MAGIC - Why it works: a log scale turns multiplication into addition. 0.1 → 1 → 10 → 100 → 1,000 ha become equally spaced steps (−1, 0, 1, 2, 3). The skewed distribution becomes much more symmetric, so the 1.5 × IQR rule behaves as designed.
# MAGIC 3. **Z-score (|z| > 3) on log10(area)** — for comparison.
# MAGIC
# MAGIC Please note that for Z-score: the mean and standard deviation are themselves inflated by extreme values, so the fence tends to sit higher than the IQR fence and flags fewer fires.

# COMMAND ----------

# DBTITLE 1,area_ha distribution
display(
    fires_checked.select("area_ha")
    .summary("count", "min", "25%", "50%", "75%", "max", "mean", "stddev")
)

# COMMAND ----------

# displays the 10 largest fires in the dataset
display(fires_checked.orderBy(F.desc("area_ha")).limit(10))

# COMMAND ----------

q1, q3 = fires_checked.approxQuantile("area_ha", [0.25, 0.75], 0.0)
raw_fence = q3 + 1.5 * (q3 - q1)

# takes base-10 logarithm of each area. This transforms the highly skewed distribution into something more symmetric: 0.1 → -1, 1 → 0, 10 → 1, 100 → 2, 1000 → 3.
positive = fires_checked.filter(F.col("area_ha") > 0).withColumn("log_ha", F.log10("area_ha"))
# lq1 = -1 (0.1 ha) & lq3 = 0 (1 ha)
lq1, lq3 = positive.approxQuantile("log_ha", [0.25, 0.75], 0.0)
# Take the Tukey IQR fence formula and apply it to the log-transformed data
# Convert the fence back from log units to hectares
log_fence_ha = 10 ** (lq3 + 1.5 * (lq3 - lq1))
mu, sd = positive.select(F.mean("log_ha"), F.stddev("log_ha")).first()
z_fence_ha = 10 ** (mu + 3 * sd)

comparison = spark.createDataFrame(
    [
        ("IQR on raw area_ha", float(raw_fence), fires_checked.filter(F.col("area_ha") > raw_fence).count()),
        ("IQR on log10(area_ha)", float(log_fence_ha), fires_checked.filter(F.col("area_ha") > log_fence_ha).count()),
        ("z-score > 3 on log10(area_ha)", float(z_fence_ha), fires_checked.filter(F.col("area_ha") > z_fence_ha).count()),
    ],
    "method STRING, upper_fence_ha DOUBLE, outliers LONG",
)
display(comparison)



# COMMAND ----------

# MAGIC %md
# MAGIC **Decision:** large fires are real events (e.g. the 2023 season), and they are exactly what the project is about, so they are **kept**.
# MAGIC They are flagged with `area_ha_outlier` using a **domain threshold of 200 ha** — the standard Canadian fire-management definition of a "large fire"
# MAGIC ([CIFFC convention](https://cwfis.cfs.nrcan.gc.ca/downloads/nfdb/fire_pnt/current_version/NFDB_point_shapefile_metadata.pdf)). This is easier to justify in a report than a statistical fence and is stable across data updates.
# MAGIC The IQR analysis above serves as supporting evidence: the log-IQR fence (358 ha) and z-score fence (10,031 ha) bracket the 200 ha threshold,
# MAGIC confirming that the domain cutoff sits in a defensible range.

# COMMAND ----------

LARGE_FIRE_THRESHOLD_HA = 200  # CIFFC convention: fires >= 200 ha are "large fires"

fires_checked = fires_checked.withColumn("area_ha_outlier", F.col("area_ha") > F.lit(LARGE_FIRE_THRESHOLD_HA))
log("fires", "outlier (domain threshold)", "area_ha",
    fires_checked.filter("area_ha_outlier").count(), f"flagged and kept (at the threshold of {LARGE_FIRE_THRESHOLD_HA} ha)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates
# MAGIC `fire_id` is unique, so there are no exact duplicates. Near-duplicates — different fire numbers with the same start date and
# MAGIC the exact same coordinates — may be one fire reported twice.
# MAGIC
# MAGIC **What we found:** 10 pairs of near-duplicate fires (20 rows total). Each pair shares the same `start_date`, `latitude`, and
# MAGIC `longitude` but has a different `area_ha`. In most pairs the larger fire has a non-zero area while its partner reports 0 ha
# MAGIC (e.g. fire `20071080504` at 549.9 ha paired with `20071080507` at 0 ha). Keeping both would give an ML model two different
# MAGIC target values (area) for the same input features (date + location), effectively double-counting the event.
# MAGIC
# MAGIC **How we proceed:** within each near-duplicate group, the record with the **larger `area_ha`** is kept and the rest are removed.
# MAGIC The display below shows both rows of every pair side by side (the `_dup_rank` column marks 1 = kept, 2 = removed).
# MAGIC This removes 10 rows (15,078 → 15,068), a 0.07% reduction with no information loss since each kept row covers the same
# MAGIC fire event at the same location and date.

# COMMAND ----------

assert fires_checked.select("fire_id").distinct().count() == fires_checked.count(), "fire_id is not unique"

# Near-duplicates: same start date and coordinates, different fire_id
w_rank = Window.partitionBy("start_date", "latitude", "longitude").orderBy(F.desc("area_ha"), "fire_id")
w_count = Window.partitionBy("start_date", "latitude", "longitude")
fires_checked = fires_checked.withColumn("_dup_rank", F.row_number().over(w_rank))

# Show ALL rows in duplicate groups (both kept and removed), side by side
display(
    fires_checked.withColumn("_dup_count", F.count("*").over(w_count))
    .filter("_dup_count > 1")
    .orderBy("start_date", "latitude", "_dup_rank")
    .drop("_dup_count")
)

n_before = fires_checked.count()
fires_checked = fires_checked.filter("_dup_rank == 1").drop("_dup_rank")
n_after = fires_checked.count()
log("fires", "near-duplicate removal", "start_date, latitude, longitude",
    n_before - n_after, f"removed smaller-area duplicates, kept larger-area record ({n_after:,} rows remaining)")

silver_fires = fires_checked

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 — Weather
# MAGIC ## 2.1 Type casting and standardization

# COMMAND ----------

MEASURES = [
    "max_temp_c", "min_temp_c", "mean_temp_c", "heat_deg_days_c", "cool_deg_days_c",
    "total_rain_mm", "total_snow_cm", "total_precip_mm", "snow_on_grnd_cm",
]
FLAGS = [
    "max_temp_flag", "min_temp_flag", "mean_temp_flag", "heat_deg_days_flag", "cool_deg_days_flag",
    "total_rain_flag", "total_snow_flag", "total_precip_flag", "snow_on_grnd_flag",
]

EMPTY_COLS = ["dir_of_max_gust_10s_deg", "spd_of_max_gust_km_h", "dir_of_max_gust_flag", "spd_of_max_gust_flag", "data_quality"]

weather_bronze = spark.table(BRONZE_WEATHER)

# Dropped: year/month/day (fully determined by date)
weather_typed = weather_bronze.select(
    clean_str("climate_id").alias("climate_id"),
    clean_str("station_name").alias("station_name"),
    clean_str("latitude_y").try_cast("double").alias("latitude"),
    clean_str("longitude_x").try_cast("double").alias("longitude"),
    F.to_date(clean_str("date_time"), "yyyy-MM-dd").alias("date"),
    *[clean_str(c).try_cast("double").alias(c) for c in MEASURES],
    *[clean_str(c).alias(c) for c in FLAGS]
)

assert weather_typed.select("climate_id", "date").distinct().count() == weather_typed.count(), "(climate_id, date) is not unique"
# 3 stations x 9,497 days 
print(f"{weather_typed.count():,} station-days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Null profiling
# MAGIC
# MAGIC Each measure column (e.g. `total_precip_mm`) has a companion flag column (e.g. `total_precip_flag`).
# MAGIC The flag is ECCC's metadata about **how each value was obtained** — not just whether it is missing:
# MAGIC
# MAGIC | Flag | Meaning | Value in measure column |
# MAGIC |---|---|---|
# MAGIC | `M` | Missing — station was operating but this specific measurement was not recorded (instrument failure, data not collected) | NULL |
# MAGIC | (no flag) | Station was not reporting at all — no data for that entire day | NULL |
# MAGIC | `E` | Estimated — value is present but was derived, not directly measured | A number |
# MAGIC | `T` | Trace — precipitation too small to measure, stored as 0.0 | 0.0 |
# MAGIC | `C` | Precipitation occurred but amount is uncertain | A number |
# MAGIC
# MAGIC ### Why the distinction matters
# MAGIC
# MAGIC A NULL in a measure column can arise in two ways, and the flag tells us which:
# MAGIC
# MAGIC 1. **Flag = `M`** → the station was active that day but the instrument failed for this variable.
# MAGIC    The gap is typically short (a day or two), so interpolating from neighbouring days is reasonable.
# MAGIC 2. **No flag (NULL)** → the station was offline entirely (not reporting any data).
# MAGIC    These gaps can span weeks or months, so interpolating would mean inventing weather fiction.
# MAGIC
# MAGIC This distinction drives the null-handling strategy in 2.4: short gaps with an `M` flag are good
# MAGIC candidates for linear interpolation, while long gaps with no flag are left null and deferred to Gold.
# MAGIC For example, station 7063560 has 3,473 missing precipitation days with no flag versus only 353 with `M` —
# MAGIC indicating the station had long offline periods rather than isolated instrument failures.

# COMMAND ----------

# Display NULL columns and its count
empty_profile = weather_bronze.select(
    *[F.sum(F.when(F.trim(F.col(c)) != "", 0).otherwise(1)).alias(c) for c in EMPTY_COLS]
)
display(empty_profile)

total_rows = weather_bronze.count()

for c, v in empty_profile.first().asDict().items():
    # Assertion to ensure that the entire column is empty before dropping it
    assert v == total_rows, f"{c} is not entirely empty"
    log("weather", "drop column", c, v, "dropped: 100% missing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: Missing counts per station for every measure column
# MAGIC Every station has 9,497 days total. Sorel (7063560) is the worst — roughly 40% missing for most columns.

# COMMAND ----------

# for each station, count how many NULLs each measure column has, and also count the total number of days
def null_summary(df, cols):
    return (
        df.groupBy("climate_id")
        .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols], F.count("*").alias("days"))
        .orderBy("climate_id")
    )

display(null_summary(weather_typed, MEASURES))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: Missing values split by whether the flag is M or absent
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC This is the key insight: for Sorel, 3,473 missing precipitation/max_temp days have **no flag** vs only 353/640 with M. That means Sorel wasn't reporting at all for long stretches — it wasn't just instrument failures on active days.

# COMMAND ----------

# Missing values with vs without the ECCC 'M' flag
display(
    weather_typed.groupBy("climate_id").agg(
        F.sum((F.col("max_temp_c").isNull() & (F.col("max_temp_flag") == "M")).cast("int")).alias("max_temp_missing_flag_M"),
        F.sum((F.col("max_temp_c").isNull() & F.col("max_temp_flag").isNull()).cast("int")).alias("max_temp_missing_no_flag"),
        F.sum((F.col("total_precip_mm").isNull() & (F.col("total_precip_flag") == "M")).cast("int")).alias("precip_missing_flag_M"),
        F.sum((F.col("total_precip_mm").isNull() & F.col("total_precip_flag").isNull()).cast("int")).alias("precip_missing_no_flag"),
    ).orderBy("climate_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 3: Missing days per station and year (max temperature) — 
# MAGIC
# MAGIC shows whether gaps are scattered or a station stopped reporting for whole years.

# COMMAND ----------

display(
    weather_typed.groupBy("station_name", F.year("date").alias("year"))
    .agg(F.sum(F.col("max_temp_c").isNull().cast("int")).alias("missing_max_temp"))
    .groupBy("year").pivot("station_name").sum("missing_max_temp")
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,Key takeaways from missing-days table
# MAGIC %md
# MAGIC ### Key takeaways from the missing-days table
# MAGIC
# MAGIC * **Lac Bouchette** — 365 missing days every year from 2016 onward. The station stopped reporting entirely in 2016; all post-2015 data is absent.
# MAGIC * **Sorel** — gaps concentrated in 2011–2015 (up to 10 days/year). Before and after that window, the station reports reliably.
# MAGIC * **Les Buissons** — scattered gaps throughout (0–311 days/year), with the worst stretch in 2012–2016. Never a full year offline.
# MAGIC
# MAGIC **Implication for null handling (2.4):** Lac Bouchette's post-2016 gaps are long offline periods (no `M` flag) and will be left null — interpolating a year of invented temperatures would be fiction. The shorter, `M`-flagged gaps at the other stations are candidates for **interpolation** or **group-mean imputation**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Station attributes (normalization)
# MAGIC Station name and coordinates repeat on all ~9,500 rows of each station. They depend only on `climate_id`
# MAGIC (functional dependency `climate_id → station_name, latitude, longitude`), so they are moved to their own table.

# COMMAND ----------

fd_check = weather_typed.groupBy("climate_id").agg(
    F.countDistinct("station_name").alias("names"),
    F.countDistinct("latitude").alias("latitudes"),
    F.countDistinct("longitude").alias("longitudes"),
)
# Check functional dependency climate_id -> station_name, lat, long
assert fd_check.filter("names > 1 OR latitudes > 1 OR longitudes > 1").count() == 0, "climate_id does not determine station attributes"


silver_stations = weather_typed.groupBy("climate_id").agg(
    F.first("station_name").alias("station_name"),
    F.first("latitude").alias("latitude"),
    F.first("longitude").alias("longitude"),
)
display(silver_stations)

weather_typed = weather_typed.drop("station_name", "latitude", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Outlier detection (observed values only)
# MAGIC
# MAGIC ### Domain rules
# MAGIC Physically impossible values are set to null, so they are handled with the other missing values in 2.4.

# COMMAND ----------

RANGES = {
    "max_temp_c": (-60, 45), "min_temp_c": (-60, 45), "mean_temp_c": (-60, 45),
    "total_rain_mm": (0, 300), "total_snow_cm": (0, 200), "total_precip_mm": (0, 300), "snow_on_grnd_cm": (0, 500),
}

weather_checked = weather_typed
for c, (lo, hi) in RANGES.items():
    bad = weather_checked.filter(~F.col(c).between(lo, hi)).count()
    log("weather", "range rule", c, bad, f"set to null (outside {lo}..{hi})" if bad else f"no values outside {lo}..{hi}")
    weather_checked = weather_checked.withColumn(c, F.when(F.col(c).between(lo, hi), F.col(c)))

# Consistency rule: daily max must not be below daily min
inconsistent = F.col("max_temp_c") < F.col("min_temp_c")
bad = weather_checked.filter(inconsistent).count()
log("weather", "consistency rule", "max_temp_c < min_temp_c", bad, "both set to null" if bad else "no violations")
weather_checked = (
    weather_checked
    .withColumn("_bad", F.coalesce(inconsistent, F.lit(False)))
    .withColumn("max_temp_c", F.when(~F.col("_bad"), F.col("max_temp_c")))
    .withColumn("min_temp_c", F.when(~F.col("_bad"), F.col("min_temp_c")))
    .drop("_bad")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temperature: global vs seasonal z-score
# MAGIC A global z-score compares a January day with a July day, so it mostly flags ordinary winter days.
# MAGIC The **seasonal z-score** compares each value with the mean and standard deviation of the same station and month.

# COMMAND ----------

w_global = Window.partitionBy("climate_id")
w_season = Window.partitionBy("climate_id", F.month("date"))

def zscore(c, w):
    return (F.col(c) - F.avg(c).over(w)) / F.stddev(c).over(w)

weather_checked = (
    weather_checked
    .withColumn("max_temp_z", zscore("max_temp_c", w_season))
    .withColumn("min_temp_z", zscore("min_temp_c", w_season))
    .withColumn("max_temp_outlier", F.coalesce(F.abs("max_temp_z") > 3, F.lit(False)))
    .withColumn("min_temp_outlier", F.coalesce(F.abs("min_temp_z") > 3, F.lit(False)))
)

display(
    weather_checked
    .withColumn("max_temp_gz", zscore("max_temp_c", w_global))
    .withColumn("min_temp_gz", zscore("min_temp_c", w_global))
    .select(
        F.sum((F.abs("max_temp_gz") > 3).cast("int")).alias("max_temp_global_z_gt3"),
        F.sum(F.col("max_temp_outlier").cast("int")).alias("max_temp_seasonal_z_gt3"),
        F.sum((F.abs("min_temp_gz") > 3).cast("int")).alias("min_temp_global_z_gt3"),
        F.sum(F.col("min_temp_outlier").cast("int")).alias("min_temp_seasonal_z_gt3"),
    )
)

display(
    weather_checked.filter("max_temp_outlier OR min_temp_outlier")
    .select("climate_id", "date", "max_temp_c", F.round("max_temp_z", 2).alias("max_temp_z"),
            "min_temp_c", F.round("min_temp_z", 2).alias("min_temp_z"))
    .orderBy(F.desc(F.greatest(F.abs("max_temp_z"), F.abs("min_temp_z"))))
    .limit(10)
)

for c in ["max_temp", "min_temp"]:
    log("weather", "outlier (seasonal z-score > 3)", f"{c}_c",
        weather_checked.filter(f"{c}_outlier").count(), "flagged and kept")

weather_checked = weather_checked.drop("max_temp_z", "min_temp_z")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Precipitation: IQR on all days vs on wet days
# MAGIC Most days are dry, so Q1 = 0 and the IQR fence on all days is only a few mm — it flags every ordinary rainy day.
# MAGIC Applying IQR to **wet days only** (precipitation > 0), per station, gives a meaningful fence for heavy-rain events.

# COMMAND ----------

pq1, pq3 = weather_checked.approxQuantile("total_precip_mm", [0.25, 0.75], 0.0)
all_days_fence = pq3 + 1.5 * (pq3 - pq1)

wet_fences = (
    weather_checked.filter(F.col("total_precip_mm") > 0)
    .groupBy("climate_id")
    .agg(F.percentile_approx("total_precip_mm", 0.25).alias("q1"), F.percentile_approx("total_precip_mm", 0.75).alias("q3"))
    .withColumn("wet_day_fence_mm", F.col("q3") + 1.5 * (F.col("q3") - F.col("q1")))
)
display(wet_fences)

weather_checked = (
    weather_checked.join(wet_fences.select("climate_id", "wet_day_fence_mm"), "climate_id")
    .withColumn("precip_outlier", F.coalesce(F.col("total_precip_mm") > F.col("wet_day_fence_mm"), F.lit(False)))
    .drop("wet_day_fence_mm")
)

display(
    spark.createDataFrame(
        [
            ("IQR on all days", float(all_days_fence), weather_checked.filter(F.col("total_precip_mm") > all_days_fence).count()),
            ("IQR on wet days, per station", None, weather_checked.filter("precip_outlier").count()),
        ],
        "method STRING, fence_mm DOUBLE, outliers LONG",
    )
)
display(weather_checked.orderBy(F.desc("total_precip_mm")).select("climate_id", "date", "total_precip_mm", "total_precip_flag").limit(10))

log("weather", "outlier (IQR on wet days)", "total_precip_mm",
    weather_checked.filter("precip_outlier").count(), "flagged and kept (heavy-rain events are real)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Null handling
# MAGIC
# MAGIC Only the attributes the model will use are imputed: max/min/mean temperature and total precipitation
# MAGIC (heating/cooling degree days are recomputed from mean temperature). Rain, snow and snow on ground keep their nulls.
# MAGIC
# MAGIC | Step | Technique | Applies to | Why |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Derivation from a functional dependency** | `total_precip = rain + snow`; `mean = (max + min) / 2`; degree days from mean | Exact, not an estimate — the FD is verified first |
# MAGIC | 2 | **Linear interpolation** within the station's time series | max/min temperature, gaps ≤ 3 days | Temperature changes smoothly from day to day |
# MAGIC | 3 | **Group mean** (same station, same month, observed values) | max/min temperature and precipitation, gaps ≤ 30 days | Keeps the seasonal level; precipitation is not interpolated because rain is not continuous |
# MAGIC | 4 | **Leave null** | gaps > 30 days | A month or more of invented weather would be fiction; Gold decides how to handle these rows |
# MAGIC
# MAGIC Each imputed column gets a `<column>_source` column recording how every value was obtained.
# MAGIC The series is handled in pandas because interpolation and gap lengths need row order within each station (28k rows fit easily in memory).

# COMMAND ----------

IMPUTE_COLS = ["max_temp_c", "min_temp_c", "mean_temp_c", "heat_deg_days_c", "cool_deg_days_c",
               "total_rain_mm", "total_snow_cm", "total_precip_mm"]

pdf = weather_checked.select("climate_id", "date", *IMPUTE_COLS).toPandas()
pdf["date"] = pd.to_datetime(pdf["date"])
pdf = pdf.sort_values(["climate_id", "date"]).reset_index(drop=True)
pdf["month"] = pdf["date"].dt.month

# One row per calendar day per station, so row order == time order
assert (pdf.groupby("climate_id")["date"].diff().dropna() == pd.Timedelta(days=1)).all(), "calendar has gaps"

SOURCE_COLS = ["max_temp_c", "min_temp_c", "mean_temp_c", "total_precip_mm"]
for c in SOURCE_COLS:
    pdf[f"{c}_source"] = np.where(pdf[c].notna(), "observed", None)

def round_half_away(x, decimals=1):
    """ECCC rounds .x5 away from zero (-11.25 -> -11.3); Python/numpy round to even. Inputs are multiples of 0.05."""
    f = 10 ** decimals
    return np.sign(x) * np.floor(np.abs(x) * f + 0.5 + 1e-6) / f

def gap_length(s):
    """For each missing value, the length of the run of consecutive missing values it belongs to (0 if observed)."""
    missing = s.isna()
    run_id = (~missing).cumsum()
    return missing.groupby(run_id).transform("sum").where(missing, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1a — Verify the functional dependencies before using them

# COMMAND ----------

full = pdf.dropna(subset=["max_temp_c", "min_temp_c", "mean_temp_c"])
mean_fd_violations = int((round_half_away((full["max_temp_c"] + full["min_temp_c"]) / 2) - full["mean_temp_c"]).abs().gt(0.01).sum())

full_p = pdf.dropna(subset=["total_rain_mm", "total_snow_cm", "total_precip_mm"])
precip_fd_violations = int((full_p["total_rain_mm"] + full_p["total_snow_cm"] - full_p["total_precip_mm"]).abs().gt(0.01).sum())

full_d = pdf.dropna(subset=["mean_temp_c", "heat_deg_days_c", "cool_deg_days_c"])
dd_fd_violations = int(
    ((np.maximum(0, 18 - full_d["mean_temp_c"]) - full_d["heat_deg_days_c"]).abs().gt(0.01)
     | (np.maximum(0, full_d["mean_temp_c"] - 18) - full_d["cool_deg_days_c"]).abs().gt(0.01)).sum()
)

print(f"mean = (max + min) / 2       : {mean_fd_violations} violations in {len(full):,} complete rows")
print(f"precip = rain + snow         : {precip_fd_violations} violations in {len(full_p):,} complete rows")
print(f"HDD = max(0, 18 - mean), CDD : {dd_fd_violations} violations in {len(full_d):,} complete rows")
assert mean_fd_violations == 0 and precip_fd_violations == 0 and dd_fd_violations == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1b — Precipitation from rain + snow

# COMMAND ----------

m = pdf["total_precip_mm"].isna() & pdf["total_rain_mm"].notna() & pdf["total_snow_cm"].notna()
pdf.loc[m, "total_precip_mm"] = pdf.loc[m, "total_rain_mm"] + pdf.loc[m, "total_snow_cm"]
pdf.loc[m, "total_precip_mm_source"] = "fd_derived"
log("weather", "impute: FD rain + snow", "total_precip_mm", m.sum(), "derived exactly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps 2–3 — Interpolation and group mean

# COMMAND ----------

def impute(col, interpolate):
    gaps = pdf.groupby("climate_id")[col].transform(gap_length)
    group_mean = pdf.groupby(["climate_id", "month"])[col].transform("mean")   # computed before this column is filled

    if interpolate:
        interp = pdf.groupby("climate_id")[col].transform(lambda s: s.interpolate(method="linear", limit_area="inside"))
        m = pdf[col].isna() & (gaps <= INTERP_MAX_GAP) & interp.notna()
        pdf.loc[m, col] = interp[m].round(1)
        pdf.loc[m, f"{col}_source"] = "interpolated"
        log("weather", f"impute: linear interpolation (gap <= {INTERP_MAX_GAP} days)", col, m.sum(), "filled")

    m = pdf[col].isna() & (gaps <= GROUP_MEAN_MAX_GAP) & group_mean.notna()
    pdf.loc[m, col] = group_mean[m].round(1)
    pdf.loc[m, f"{col}_source"] = "station_month_mean"
    log("weather", f"impute: station x month mean (gap <= {GROUP_MEAN_MAX_GAP} days)", col, m.sum(), "filled")

    m = pdf[col].isna()
    pdf.loc[m, f"{col}_source"] = "not_imputed"
    log("weather", f"leave null (gap > {GROUP_MEAN_MAX_GAP} days)", col, m.sum(), "left null")

impute("max_temp_c", interpolate=True)
impute("min_temp_c", interpolate=True)
impute("total_precip_mm", interpolate=False)

# Max and min are imputed independently, which can produce max < min on a day where one of them was observed.
# Re-apply the consistency rule: the imputed value is clipped to the observed one.
bad = pdf["max_temp_c"] < pdf["min_temp_c"]
fix_max = bad & (pdf["max_temp_c_source"] != "observed")
fix_min = bad & ~fix_max & (pdf["min_temp_c_source"] != "observed")
pdf.loc[fix_max, "max_temp_c"] = pdf.loc[fix_max, "min_temp_c"]
pdf.loc[fix_min, "min_temp_c"] = pdf.loc[fix_min, "max_temp_c"]
log("weather", "consistency after imputation", "max_temp_c < min_temp_c", (fix_max | fix_min).sum(), "imputed value clipped to observed value")
assert not (pdf["max_temp_c"] < pdf["min_temp_c"]).any()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1c — Mean temperature and degree days from the (now filled) max/min

# COMMAND ----------

m = pdf["mean_temp_c"].isna() & pdf["max_temp_c"].notna() & pdf["min_temp_c"].notna()
both_observed = (pdf["max_temp_c_source"] == "observed") & (pdf["min_temp_c_source"] == "observed")
pdf.loc[m, "mean_temp_c"] = round_half_away((pdf.loc[m, "max_temp_c"] + pdf.loc[m, "min_temp_c"]) / 2)
pdf.loc[m & both_observed, "mean_temp_c_source"] = "fd_derived"
pdf.loc[m & ~both_observed, "mean_temp_c_source"] = "fd_from_imputed"
pdf.loc[pdf["mean_temp_c"].isna(), "mean_temp_c_source"] = "not_imputed"
log("weather", "impute: FD (max + min) / 2 from observed max/min", "mean_temp_c", (m & both_observed).sum(), "derived exactly")
log("weather", "impute: FD (max + min) / 2 from imputed max/min", "mean_temp_c", (m & ~both_observed).sum(), "derived")

for c, expr in [("heat_deg_days_c", lambda t: np.maximum(0, 18 - t)), ("cool_deg_days_c", lambda t: np.maximum(0, t - 18))]:
    m = pdf[c].isna() & pdf["mean_temp_c"].notna()
    pdf.loc[m, c] = expr(pdf.loc[m, "mean_temp_c"]).round(1)
    log("weather", "impute: FD from mean temperature", c, m.sum(), "derived")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Back to Spark, and before/after comparison

# COMMAND ----------

out_cols = ["climate_id", "date", *IMPUTE_COLS, *[f"{c}_source" for c in SOURCE_COLS]]
imputed = spark.createDataFrame(pdf[out_cols])
imputed = imputed.select(
    "climate_id",
    F.col("date").cast("date").alias("date"),
    # pandas NaN arrives as NaN, not null
    *[F.when(~F.isnan(c), F.col(c)).alias(c) for c in IMPUTE_COLS],
    *[f"{c}_source" for c in SOURCE_COLS],
)

silver_weather = weather_checked.drop(*IMPUTE_COLS).join(imputed, ["climate_id", "date"])
assert silver_weather.count() == weather_checked.count()

before = null_summary(weather_typed, SOURCE_COLS).withColumn("stage", F.lit("before"))
after = null_summary(silver_weather, SOURCE_COLS).withColumn("stage", F.lit("after"))
display(before.unionByName(after).orderBy("climate_id", F.desc("stage")))

display(
    silver_weather.groupBy("climate_id").pivot("max_temp_c_source").count().orderBy("climate_id")
)
display(
    silver_weather.groupBy("climate_id").pivot("total_precip_mm_source").count().orderBy("climate_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3 — Write Silver tables
# MAGIC
# MAGIC Primary keys and the foreign key `weather_daily.climate_id → weather_stations.climate_id` are declared so Unity Catalog
# MAGIC can show the relationships (informational constraints — not enforced on write).

# COMMAND ----------

T_FIRES = f"{SILVER}.fires"
T_STATIONS = f"{SILVER}.weather_stations"
T_WEATHER = f"{SILVER}.weather_daily"
T_LOG = f"{SILVER}.cleaning_log"

# Drop the FK first, otherwise the stations table (and its PK) cannot be replaced on a re-run
if spark.catalog.tableExists(T_WEATHER):
    spark.sql(f"ALTER TABLE {T_WEATHER} DROP CONSTRAINT IF EXISTS fk_weather_station")

def write_table(df, name, pk_cols, comment):
    df.createOrReplaceTempView("_to_write")
    spark.sql(f"CREATE OR REPLACE TABLE {name} COMMENT '{comment}' AS SELECT * FROM _to_write")
    for c in pk_cols:
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN {c} SET NOT NULL")
    pk_name = "pk_" + name.split(".")[-1]
    spark.sql(f"ALTER TABLE {name} DROP CONSTRAINT IF EXISTS {pk_name}")
    spark.sql(f"ALTER TABLE {name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk_cols)})")
    print(f"{name}: {spark.table(name).count():,} rows")

write_table(silver_fires, T_FIRES, ["fire_id"], "SOPFEU wildfire origin points 2000-2024, typed and checked")
write_table(silver_stations, T_STATIONS, ["climate_id"], "ECCC weather stations (one row per station)")
write_table(silver_weather, T_WEATHER, ["climate_id", "date"], "ECCC daily weather 2000-2025, typed, outliers flagged, nulls handled")

spark.sql(f"""
    ALTER TABLE {T_WEATHER} ADD CONSTRAINT fk_weather_station
    FOREIGN KEY (climate_id) REFERENCES {T_STATIONS} (climate_id)
""")

log_df = spark.createDataFrame(cleaning_log, "dataset STRING, step STRING, column STRING, rows_affected LONG, action STRING")
log_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_LOG)
display(spark.table(T_LOG))