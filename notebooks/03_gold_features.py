# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 — Gold: Integration and Features
# MAGIC
# MAGIC Joins fires to weather (spatial + temporal join) and builds the two model tables. **Intensive zone only**: Nordic-zone
# MAGIC fires are a median 399 km from the nearest station, too far for representative weather (see README, Scope).
# MAGIC
# MAGIC | Output (Gold) | One row = | Key | Used by |
# MAGIC |---|---|---|---|
# MAGIC | `gold.weather_lookback` | one station on one day, with the weather of the days **before** it | (`station`, `date`) | both tables below, future scoring |
# MAGIC | `gold.grid_cells` | one 0.5° grid cell that had at least one Intensive-zone fire (`in_model` marks the cells used by Model 1) | `cell_id` | dashboard map |
# MAGIC | `gold.fire_weather` | one Intensive-zone fire with the weather before its start date | `fire_id` | Model 2 (cause) |
# MAGIC | `gold.hotspot_features` | one grid cell in one week (April–October) | (`cell_id`, `week_start`) | Model 1 (hotspot) |
# MAGIC
# MAGIC **No feature looks at the period being predicted.** Every weather and fire-history feature is computed from days strictly
# MAGIC before the reference date: the fire's start date in `fire_weather`, the Monday that starts the week in `hotspot_features`.
# MAGIC Example: the row for the week of June 5–11 uses weather up to Sunday June 4; June 5–11's own weather first appears as the
# MAGIC "previous 7 days" of the June 12–18 row.

# COMMAND ----------

import math

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "wildfire_project"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

GRID_DEG = 0.5            # grid cell size in degrees (about 55 km north-south x 39 km east-west in southern Québec)
SEASON_MONTHS = (4, 10)   # weeks starting April to October (98.9% of Intensive-zone fires)
WINDOWS = [7, 14, 30]     # look-back windows in days
MIN_COVERAGE = 0.8        # a window feature needs a value on at least 80% of its days, otherwise it is null
RAIN_DAY_MM = 1.0         # a "rain day" has at least 1 mm (less barely wets the fuel)
MAX_STATION_KM = 300      # a station further than this is never used (99.5% of Intensive fires have one within 300 km)

def min_days(n):
    return math.ceil(MIN_COVERAGE * n)

def write_table(df, name, pk_cols, comment):
    """Create or replace a Unity Catalog table with a comment and a primary key (informational)."""
    df.createOrReplaceTempView("_to_write")
    spark.sql(f"CREATE OR REPLACE TABLE {name} COMMENT '{comment}' AS SELECT * FROM _to_write")
    for c in pk_cols:
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN {c} SET NOT NULL")
    pk_name = "pk_" + name.split(".")[-1]
    spark.sql(f"ALTER TABLE {name} DROP CONSTRAINT IF EXISTS {pk_name}")
    spark.sql(f"ALTER TABLE {name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk_cols)})")
    print(f"{name}: {spark.table(name).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1 — Weather before each day (`gold.weather_lookback`)
# MAGIC
# MAGIC For every station and day D, summarises the days **before** D (D itself is excluded). Silver guarantees one row per
# MAGIC station per calendar day, so "the 7 rows before" is exactly "the 7 days before".
# MAGIC
# MAGIC | Feature | Meaning | Why |
# MAGIC |---|---|---|
# MAGIC | `precip_7d_mm`, `precip_14d_mm`, `precip_30d_mm` | Total precipitation over the 7 / 14 / 30 days before D | Short vs long dry spells: 7 days dries surface litter, 30 days dries deeper fuel |
# MAGIC | `days_since_rain` | Days since the last day with ≥ 1 mm (1 = it rained the day before) | Length of the current dry spell |
# MAGIC | `max_temp_7d_mean_c` | Mean daily high over the 7 days before D | A run of hot days dries fuel faster than one hot day |
# MAGIC | `windy_days_7d` | Days with a gust ≥ 31 km/h in the 7 days before D | Wind dries fuel and spreads fire. A `<31` day counts as calm, not missing |
# MAGIC | `*_observed_share` | Share of the window's days whose value was observed rather than imputed in Silver | Shows how much of a feature rests on filled values |
# MAGIC
# MAGIC A window feature is null when fewer than 80% of its days have a value. `days_since_rain` is null when there is a
# MAGIC precipitation gap after the last rain day, because the dry spell's length is then unknown.

# COMMAND ----------

weather = spark.table(f"{SILVER}.weather_daily").select(
    "station", "date", "max_temp_c", "max_temp_c_source", "total_precip_mm", "total_precip_mm_source",
    "spd_of_max_gust_km_h", "spd_of_max_gust_below_31",
)

def before(n):
    """The n days before the current row (the current day is excluded)."""
    return Window.partitionBy("station").orderBy("date").rowsBetween(-n, -1)

all_before = Window.partitionBy("station").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)

precip_observed = (F.col("total_precip_mm_source") == "observed").cast("int")
gust_known = F.col("spd_of_max_gust_km_h").isNotNull() | F.col("spd_of_max_gust_below_31")

cols = []
for n in WINDOWS:
    w = before(n)
    cols += [
        F.when(F.count("total_precip_mm").over(w) >= min_days(n), F.round(F.sum("total_precip_mm").over(w), 1)).alias(f"precip_{n}d_mm"),
        F.round(F.avg(precip_observed).over(w), 2).alias(f"precip_{n}d_observed_share"),
    ]

w7 = before(7)
last_rain = F.max(F.when(F.col("total_precip_mm") >= RAIN_DAY_MM, F.col("date"))).over(all_before)
last_gap = F.max(F.when(F.col("total_precip_mm").isNull(), F.col("date"))).over(all_before)
cols += [
    F.when(last_rain.isNotNull() & (last_gap.isNull() | (last_gap < last_rain)), F.datediff("date", last_rain)).alias("days_since_rain"),
    F.when(F.count("max_temp_c").over(w7) >= min_days(7), F.round(F.avg("max_temp_c").over(w7), 1)).alias("max_temp_7d_mean_c"),
    F.round(F.avg((F.col("max_temp_c_source") == "observed").cast("int")).over(w7), 2).alias("max_temp_7d_observed_share"),
    F.when(F.sum(gust_known.cast("int")).over(w7) >= min_days(7),
           F.sum((F.col("spd_of_max_gust_km_h") >= 31).cast("int")).over(w7)).alias("windy_days_7d"),
]
lookback = weather.select("station", "date", *cols)

# A station "has weather" on a day when its core features exist; otherwise the next-nearest station is used (Part 3)
lookback = lookback.withColumn("has_weather", F.col("precip_7d_mm").isNotNull() & F.col("max_temp_7d_mean_c").isNotNull())

# Written now and read back, so later parts do not recompute the windows (serverless cannot cache)
write_table(lookback, f"{GOLD}.weather_lookback", ["station", "date"],
            "Per station and day: weather of the days before it (7/14/30-day windows)")
lookback = spark.table(f"{GOLD}.weather_lookback")

WEATHER_FEATURES = [c for c in lookback.columns if c not in ("station", "date", "has_weather")]
display(lookback.filter("date = '2023-06-05'").orderBy("station"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 — Grid cells
# MAGIC
# MAGIC Each fire is assigned to a 0.5° × 0.5° cell, named by its south-west corner: a fire at 45.1289 N, 72.1135 W is in
# MAGIC cell `45.0_-72.5` (latitude 45.0–45.5, longitude −72.5 to −72.0).
# MAGIC
# MAGIC The model covers the cells that had at least one Intensive-zone fire in 2000–2024. Choosing cells with all years'
# MAGIC fires lets a little information from the test years into the cell list; it is accepted because almost every cell has
# MAGIC fires in many years, and the alternative (no zone boundary data) would include large areas that never burn.
# MAGIC
# MAGIC 16 remote cells (eastern Côte-Nord and the far north, 40 fires in 25 years) have no station within 300 km. Like the
# MAGIC Nordic zone, they have no representative weather, so they are marked `in_model = false` and left out of
# MAGIC `hotspot_features`. Their fires stay in `fire_weather`, where the weather columns are null.

# COMMAND ----------

fires = (
    spark.table(f"{SILVER}.fires")
    .filter(F.col("section") == "Intensive")
    .withColumn("cell_lat", F.floor(F.col("latitude") / GRID_DEG) * GRID_DEG)
    .withColumn("cell_lon", F.floor(F.col("longitude") / GRID_DEG) * GRID_DEG)
    .withColumn("cell_id", F.format_string("%.1f_%.1f", "cell_lat", "cell_lon"))
    .withColumn("week_start", F.to_date(F.date_trunc("week", "start_date")))   # Monday of the fire's week
)

stations = spark.table(f"{SILVER}.weather_stations").select(
    "station", F.col("latitude").alias("station_lat"), F.col("longitude").alias("station_lon")
)

def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two points given in degrees."""
    dlat = F.radians(F.col(lat2) - F.col(lat1))
    dlon = F.radians(F.col(lon2) - F.col(lon1))
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2))) * F.pow(F.sin(dlon / 2), 2)
    return 2 * 6371 * F.asin(F.sqrt(a))

grid_cells = (
    fires.groupBy("cell_id", "cell_lat", "cell_lon")
    .agg(
        F.count("*").alias("fires_2000_2024"),
        F.sum((F.col("cause") == "Lightning").cast("int")).alias("lightning_fires_2000_2024"),
    )
    .withColumn("center_lat", F.col("cell_lat") + GRID_DEG / 2)
    .withColumn("center_lon", F.col("cell_lon") + GRID_DEG / 2)
)

# Nearest station to the cell centre (the station actually used can differ week to week, see Part 3)
w_cell = Window.partitionBy("cell_id").orderBy("distance_km")
nearest_to_cell = (
    grid_cells.select("cell_id", "center_lat", "center_lon").crossJoin(F.broadcast(stations))
    .withColumn("distance_km", F.round(haversine_km("center_lat", "center_lon", "station_lat", "station_lon"), 1))
    .withColumn("r", F.row_number().over(w_cell)).filter("r = 1")
    .select("cell_id", F.col("station").alias("nearest_station"), F.col("distance_km").alias("nearest_station_km"))
)
# Cells with no station within MAX_STATION_KM have no representative weather, like the Nordic zone: kept on the map,
# left out of the hotspot model
grid_cells = (grid_cells.join(nearest_to_cell, "cell_id")
              .withColumn("in_model", F.col("nearest_station_km") <= MAX_STATION_KM))

write_table(grid_cells, f"{GOLD}.grid_cells", ["cell_id"],
            "0.5 degree grid cells with at least one Intensive-zone fire 2000-2024")
grid_cells = spark.table(f"{GOLD}.grid_cells")

print(f"{fires.count():,} Intensive-zone fires in {grid_cells.count()} cells")
display(grid_cells.groupBy("in_model").agg(F.count("*").alias("cells"), F.sum("fires_2000_2024").alias("fires")))
display(grid_cells.orderBy(F.desc("fires_2000_2024")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3 — Matching a place and date to a station
# MAGIC
# MAGIC The nearest station is not always usable: some stations have gaps (e.g. Les Buissons in 2013 and 2015). For each
# MAGIC point (a fire, or a cell centre in a given week), the **nearest station that has weather on the reference date** is used,
# MAGIC within 300 km. The overall nearest station is kept too, so every fallback is visible.

# COMMAND ----------

available = lookback.filter("has_weather").select("station", F.col("date").alias("asof_date"))

def match_station(points, keys):
    """points: `keys` + lat, lon, asof_date. Returns keys + nearest_station(_km) and station(_km), the nearest station with
    weather on asof_date (null if none within MAX_STATION_KM)."""
    dist = (
        points.select(*keys, "lat", "lon", "asof_date").crossJoin(F.broadcast(stations))
        .withColumn("distance_km", F.round(haversine_km("lat", "lon", "station_lat", "station_lon"), 1))
    )
    by_distance = Window.partitionBy(*keys).orderBy("distance_km")
    nearest = (
        dist.withColumn("r", F.row_number().over(by_distance)).filter("r = 1")
        .select(*keys, F.col("station").alias("nearest_station"), F.col("distance_km").alias("nearest_station_km"))
    )
    used = (
        dist.filter(F.col("distance_km") <= MAX_STATION_KM).join(available, ["station", "asof_date"])
        .withColumn("r", F.row_number().over(by_distance)).filter("r = 1")
        .select(*keys, "station", F.col("distance_km").alias("station_km"))
    )
    return nearest.join(used, keys, "left")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4 — Fire history per cell and week
# MAGIC
# MAGIC Counts are built on a full weekly calendar (all weeks of the year, so a March fire still counts as history), then
# MAGIC every feature looks only at earlier weeks.
# MAGIC
# MAGIC | Feature | Meaning |
# MAGIC |---|---|
# MAGIC | `fires_prev_4wk` | Fires in the cell in the 4 weeks before this week |
# MAGIC | `fires_prev_52wk` | Fires in the cell in the 52 weeks before this week (null in the first year) |
# MAGIC | `cell_fire_rate_prior` | Fires per year in the cell over all earlier weeks since 2000 (null in the first year) |
# MAGIC | `cell_lightning_share_prior` | Share of the cell's earlier fires caused by lightning (null if it had none) |

# COMMAND ----------

all_weeks = spark.sql("SELECT explode(sequence(DATE'2000-01-03', DATE'2024-12-30', INTERVAL 7 DAYS)) AS week_start")  # Mondays

fire_weeks = fires.groupBy("cell_id", "week_start").agg(
    F.count("*").alias("fire_count"),
    F.sum((F.col("cause") == "Lightning").cast("int")).alias("lightning_count"),
    F.sum((F.col("cause") == "Human").cast("int")).alias("human_count"),
)

calendar = (
    grid_cells.select("cell_id").crossJoin(all_weeks)
    .join(fire_weeks, ["cell_id", "week_start"], "left")
    .fillna(0, ["fire_count", "lightning_count", "human_count"])
)

w_week = Window.partitionBy("cell_id").orderBy("week_start")
prev4, prev52 = w_week.rowsBetween(-4, -1), w_week.rowsBetween(-52, -1)
prior = w_week.rowsBetween(Window.unboundedPreceding, -1)

history = calendar.select(
    "cell_id", "week_start", "fire_count", "lightning_count", "human_count",
    F.sum("fire_count").over(prev4).alias("fires_prev_4wk"),
    F.when(F.count("*").over(prev52) == 52, F.sum("fire_count").over(prev52)).alias("fires_prev_52wk"),
    F.when(F.count("*").over(prior) >= 52,
           F.round(F.sum("fire_count").over(prior) / (F.count("*").over(prior) / 52.18), 2)).alias("cell_fire_rate_prior"),
    F.when(F.sum("fire_count").over(prior) > 0,
           F.round(F.sum("lightning_count").over(prior) / F.sum("fire_count").over(prior), 2)).alias("cell_lightning_share_prior"),
).fillna(0, ["fires_prev_4wk"])

HISTORY_FEATURES = ["fires_prev_4wk", "fires_prev_52wk", "cell_fire_rate_prior", "cell_lightning_share_prior"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 5 — `gold.fire_weather` (Model 2: cause)
# MAGIC
# MAGIC The spatial join (nearest station with data) and the temporal join (that station's look-back as of the fire's start
# MAGIC date), plus the cell's fire history as of the fire's week. Label: `is_lightning`.

# COMMAND ----------

fire_points = fires.select("fire_id", F.col("latitude").alias("lat"), F.col("longitude").alias("lon"),
                           F.col("start_date").alias("asof_date"))
fire_station = match_station(fire_points, ["fire_id"])

fire_weather = (
    fires.select(
        "fire_id", "start_date", "year", F.month("start_date").alias("month"), F.dayofyear("start_date").alias("day_of_year"),
        "week_start", "cell_id", "latitude", "longitude", "cause", (F.col("cause") == "Lightning").alias("is_lightning"),
        "area_ha", "area_ha_outlier",
    )
    .join(fire_station, "fire_id", "left")
    .withColumn("used_fallback_station", F.col("station") != F.col("nearest_station"))
    .join(lookback.drop("has_weather").withColumnRenamed("date", "start_date"), ["station", "start_date"], "left")
    .join(history.select("cell_id", "week_start", *HISTORY_FEATURES), ["cell_id", "week_start"], "left")
)

write_table(fire_weather, f"{GOLD}.fire_weather", ["fire_id"],
            "Intensive-zone fires 2000-2024 with weather before the start date and cell history (Model 2)")
fire_weather = spark.table(f"{GOLD}.fire_weather")

n_fires = fires.count()
assert fire_weather.count() == n_fires, "fire_weather must have one row per Intensive-zone fire"
assert fire_weather.select("fire_id").distinct().count() == n_fires, "fire_id is not unique"

match = fire_weather.agg(
    F.count("*").alias("fires"),
    F.round(F.avg(F.col("station").isNotNull().cast("int")) * 100, 1).alias("matched_pct"),
    F.round(F.avg(F.col("used_fallback_station").cast("int")) * 100, 1).alias("fallback_pct"),
    F.percentile_approx("station_km", 0.5).alias("median_station_km"),
    F.percentile_approx("station_km", 0.9).alias("p90_station_km"),
)
display(match)

# COMMAND ----------

# MAGIC %md
# MAGIC **Example (spatial + temporal join).** The first fire in the README example, `20221080001` (2022-04-06), and two others.

# COMMAND ----------

display(
    fire_weather.filter(F.col("fire_id").isin("20221080001"))
    .unionByName(fire_weather.filter("is_lightning AND year = 2023").orderBy("start_date").limit(2))
    .select("fire_id", "start_date", "cause", "cell_id", "nearest_station", "nearest_station_km", "station", "station_km",
            "precip_7d_mm", "days_since_rain", "max_temp_7d_mean_c", "windy_days_7d", "fires_prev_52wk")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 6 — `gold.hotspot_features` (Model 1: hotspot)
# MAGIC
# MAGIC One row per cell per week whose Monday falls in April–October, 2000–2024, including weeks with no fire.
# MAGIC Label: `has_fire` (at least one Intensive-zone fire started in the cell that week). Weather comes from the nearest
# MAGIC station with data **as of the Monday**, i.e. the days up to the Sunday before.

# COMMAND ----------

model_cells = grid_cells.filter("in_model")
season = history.filter(F.month("week_start").between(*SEASON_MONTHS)).join(model_cells.select("cell_id"), "cell_id")

cell_points = (
    season.select("cell_id", "week_start").join(grid_cells.select("cell_id", "center_lat", "center_lon"), "cell_id")
    .select("cell_id", "week_start", F.col("center_lat").alias("lat"), F.col("center_lon").alias("lon"),
            F.col("week_start").alias("asof_date"))
)
cell_station = match_station(cell_points, ["cell_id", "week_start"])

hotspot_features = (
    season
    .join(grid_cells.select("cell_id", "center_lat", "center_lon"), "cell_id")
    .withColumn("year", F.year("week_start"))
    .withColumn("week_of_year", F.weekofyear("week_start"))
    .withColumn("month", F.month("week_start"))
    .withColumn("has_fire", F.col("fire_count") > 0)
    .join(cell_station.drop("nearest_station", "nearest_station_km"), ["cell_id", "week_start"], "left")
    .join(lookback.drop("has_weather").withColumnRenamed("date", "week_start"), ["station", "week_start"], "left")
    .select(
        "cell_id", "week_start", "year", "month", "week_of_year", "center_lat", "center_lon", "station", "station_km",
        *WEATHER_FEATURES, *HISTORY_FEATURES, "fire_count", "lightning_count", "human_count", "has_fire",
    )
)

write_table(hotspot_features, f"{GOLD}.hotspot_features", ["cell_id", "week_start"],
            "Cell x week (Apr-Oct) panel with backward-looking weather and fire history, label has_fire (Model 1)")
hotspot_features = spark.table(f"{GOLD}.hotspot_features")

n_cells, n_weeks = model_cells.count(), season.select("week_start").distinct().count()
n_rows = hotspot_features.count()
assert n_rows == n_cells * n_weeks, "expected one row per cell per season week"
assert hotspot_features.select("cell_id", "week_start").distinct().count() == n_rows, "(cell_id, week_start) is not unique"

# Fires whose week starts outside April–October have no row (late March / early November), nor do fires in cells left out
in_season = (fires.filter(F.month("week_start").between(*SEASON_MONTHS))
             .join(model_cells.select("cell_id"), "cell_id").count())
assert hotspot_features.agg(F.sum("fire_count")).first()[0] == in_season, "fire counts do not add up"
print(f"{n_cells} cells x {n_weeks} weeks = {n_rows:,} rows")
print(f"{in_season:,} of {n_fires:,} fires are in the panel; the rest start in a week outside April–October or in a cell left out")

display(hotspot_features.agg(
    F.round(F.avg(F.col("has_fire").cast("int")) * 100, 1).alias("positive_pct"),
    F.round(F.avg(F.col("station").isNotNull().cast("int")) * 100, 1).alias("matched_pct"),
    *[F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 1).alias(f"{c}_null_pct")
      for c in ["precip_7d_mm", "precip_30d_mm", "days_since_rain", "max_temp_7d_mean_c", "windy_days_7d"]],
))

# COMMAND ----------

# MAGIC %md
# MAGIC **Example.** Four consecutive weeks for one cell. Each row's weather and history describe the time *before* its week;
# MAGIC `has_fire` describes the week itself.

# COMMAND ----------

display(
    hotspot_features.filter("cell_id = '45.0_-72.5' AND week_start BETWEEN '2023-05-29' AND '2023-06-19'")
    .select("cell_id", "week_start", "station", "precip_7d_mm", "precip_30d_mm", "days_since_rain", "max_temp_7d_mean_c",
            "fires_prev_4wk", "fire_count", "has_fire")
    .orderBy("week_start")
)
