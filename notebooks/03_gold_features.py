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
# MAGIC | `gold.grid_cells` | one 0.5° grid cell that had at least one Intensive-zone fire (`in_model` marks the cells used by the models) | `cell_id` | dashboard map |
# MAGIC | `gold.hotspot_features` | one grid cell in one week (April–October) | (`cell_id`, `week_start`) | Model 1 (hotspot) |
# MAGIC | `gold.fire_weather` | one fire, with the features of its cell's week | `fire_id` | Model 2 (cause) |
# MAGIC
# MAGIC **Everything is as of Monday morning.** The models are used each Monday: Model 1 flags the cells likely to have a fire
# MAGIC that week, then Model 2 estimates the likely cause in those cells. So every feature describes the time **before** the
# MAGIC week's Monday. Example: the row for the week of June 5–11 uses weather up to Sunday June 4; June 5–11's own weather first
# MAGIC appears in the June 12–18 row. Model 2 is trained on the same Monday features, because on a Monday no fire has started yet.

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
MAX_STATION_KM = 300      # a station further than this is never used

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
# MAGIC # Part 1 — Weather before each Monday
# MAGIC
# MAGIC For every station and Monday, summarises the days **before** that Monday. The sums need the daily series as input
# MAGIC (Silver guarantees one row per station per day, so "the 7 rows before" is exactly "the 7 days before"); only the
# MAGIC Monday results are kept. This is an intermediate step, not a saved table.
# MAGIC
# MAGIC | Feature | Meaning | Why |
# MAGIC |---|---|---|
# MAGIC | `precip_7d_mm`, `precip_14d_mm`, `precip_30d_mm` | Total precipitation over the 7 / 14 / 30 days before Monday | Short vs long dry spells: 7 days dries surface litter, 30 days dries deeper fuel |
# MAGIC | `days_since_rain` | Days since the last day with ≥ 1 mm (1 = it rained on Sunday) | Length of the current dry spell |
# MAGIC | `max_temp_7d_mean_c` | Mean daily high over the 7 days before Monday | A run of hot days dries fuel faster than one hot day |
# MAGIC | `windy_days_7d` | Days with a gust ≥ 31 km/h in the 7 days before Monday | Wind dries fuel and spreads fire. A `<31` day counts as calm, not missing |
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

monday_weather = (
    weather.select("station", "date", *cols)
    .filter((F.dayofweek("date") == 2) & F.month("date").between(*SEASON_MONTHS))   # dayofweek: Sunday = 1, Monday = 2
    .withColumnRenamed("date", "week_start")
    # A station "has weather" on a Monday when its core features exist; otherwise the next-nearest station is used (Part 3)
    .withColumn("has_weather", F.col("precip_7d_mm").isNotNull() & F.col("max_temp_7d_mean_c").isNotNull())
)

WEATHER_FEATURES = [c for c in monday_weather.columns if c not in ("station", "week_start", "has_weather")]
display(monday_weather.filter("week_start = '2023-06-05'").orderBy("station"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 — Grid cells (`gold.grid_cells`)
# MAGIC
# MAGIC Each fire is assigned to a 0.5° × 0.5° cell, named by its south-west corner: a fire at 45.1289 N, 72.1135 W is in
# MAGIC cell `45.0_-72.5` (latitude 45.0–45.5, longitude −72.5 to −72.0). The table lists each cell once, with its centre and
# MAGIC nearest station, so the dashboard can place predictions on a map by `cell_id`.
# MAGIC
# MAGIC The models cover the cells that had at least one Intensive-zone fire in 2000–2024. Choosing cells with all years'
# MAGIC fires lets a little information from the test years into the cell list; it is accepted because almost every cell has
# MAGIC fires in many years, and the alternative (no zone boundary data) would include large areas that never burn.
# MAGIC
# MAGIC 16 remote cells (eastern Côte-Nord and the far north, 40 fires in 25 years) have no station within 300 km. Like the
# MAGIC Nordic zone, they have no representative weather, so they are marked `in_model = false` and left out of both model tables.

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
grid_cells = (grid_cells.join(nearest_to_cell, "cell_id")
              .withColumn("in_model", F.col("nearest_station_km") <= MAX_STATION_KM))

write_table(grid_cells, f"{GOLD}.grid_cells", ["cell_id"],
            "0.5 degree grid cells with at least one Intensive-zone fire 2000-2024")
grid_cells = spark.table(f"{GOLD}.grid_cells")

print(f"{fires.count():,} Intensive-zone fires in {grid_cells.count()} cells")
display(grid_cells.groupBy("in_model").agg(F.count("*").alias("cells"), F.sum("fires_2000_2024").alias("fires")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3 — Matching a cell and week to a station
# MAGIC
# MAGIC The nearest station is not always usable: some stations have gaps (e.g. Les Buissons in 2013 and 2015). For each cell
# MAGIC and week, the weather comes from the **nearest station to the cell centre that has weather on that Monday**, within 300 km.

# COMMAND ----------

available = monday_weather.filter("has_weather").select("station", "week_start")

def match_station(cell_weeks):
    """cell_weeks: cell_id, week_start, lat, lon. Returns cell_id, week_start, station, station_km: the nearest station with
    weather on that Monday (null if none within MAX_STATION_KM)."""
    keys = ["cell_id", "week_start"]
    return (
        cell_weeks.crossJoin(F.broadcast(stations))
        .withColumn("station_km", F.round(haversine_km("lat", "lon", "station_lat", "station_lon"), 1))
        .filter(F.col("station_km") <= MAX_STATION_KM)
        .join(available, ["station", "week_start"])
        .withColumn("r", F.row_number().over(Window.partitionBy(*keys).orderBy("station_km"))).filter("r = 1")
        .select(*keys, "station", "station_km")
    )

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
# MAGIC # Part 5 — `gold.hotspot_features` (Model 1: hotspot)
# MAGIC
# MAGIC One row per cell per week whose Monday falls in April–October, 2000–2024, including weeks with no fire.
# MAGIC Label: `has_fire` (at least one Intensive-zone fire started in the cell that week). This is where the spatial join
# MAGIC (cell → station) and the temporal join (station → weather before that Monday) happen.

# COMMAND ----------

model_cells = grid_cells.filter("in_model").select("cell_id", F.col("center_lat"), F.col("center_lon"))
season = history.filter(F.month("week_start").between(*SEASON_MONTHS)).join(model_cells, "cell_id")

cell_station = match_station(
    season.select("cell_id", "week_start", F.col("center_lat").alias("lat"), F.col("center_lon").alias("lon"))
)

hotspot_features = (
    season
    .withColumn("year", F.year("week_start"))
    .withColumn("week_of_year", F.weekofyear("week_start"))
    .withColumn("month", F.month("week_start"))
    .withColumn("has_fire", F.col("fire_count") > 0)
    .join(cell_station, ["cell_id", "week_start"], "left")
    .join(monday_weather.drop("has_weather"), ["station", "week_start"], "left")
    .select(
        "cell_id", "week_start", "year", "month", "week_of_year", "center_lat", "center_lon", "station", "station_km",
        *WEATHER_FEATURES, *HISTORY_FEATURES, "fire_count", "lightning_count", "human_count", "has_fire",
    )
)

write_table(hotspot_features, f"{GOLD}.hotspot_features", ["cell_id", "week_start"],
            "Cell x week (Apr-Oct) panel with weather and fire history as of Monday, label has_fire (Model 1)")
hotspot_features = spark.table(f"{GOLD}.hotspot_features")

n_cells, n_weeks = model_cells.count(), season.select("week_start").distinct().count()
n_rows = hotspot_features.count()
assert n_rows == n_cells * n_weeks, "expected one row per cell per season week"
assert hotspot_features.select("cell_id", "week_start").distinct().count() == n_rows, "(cell_id, week_start) is not unique"

# Fires whose week starts outside April–October have no row (late March / early November), nor do fires in cells left out
n_fires = fires.count()
in_panel = fires.filter(F.month("week_start").between(*SEASON_MONTHS)).join(model_cells, "cell_id").count()
assert hotspot_features.agg(F.sum("fire_count")).first()[0] == in_panel, "fire counts do not add up"
print(f"{n_cells} cells x {n_weeks} weeks = {n_rows:,} rows")
print(f"{in_panel:,} of {n_fires:,} fires are in the panel; the rest start in a week outside April–October or in a cell left out")

display(hotspot_features.agg(
    F.round(F.avg(F.col("has_fire").cast("int")) * 100, 1).alias("positive_pct"),
    F.round(F.avg(F.col("station").isNotNull().cast("int")) * 100, 1).alias("matched_pct"),
    F.percentile_approx("station_km", 0.5).alias("median_station_km"),
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 6 — `gold.fire_weather` (Model 2: cause)
# MAGIC
# MAGIC One row per fire that has a row in `hotspot_features`, carrying that cell-week's features. Label: `is_lightning`.
# MAGIC
# MAGIC Model 2's inputs are the cell-week columns (station, weather, history, `center_lat`/`center_lon`, `week_of_year`, `month`).
# MAGIC The fire's own `start_date`, `latitude`, `longitude` and `area_ha` are kept for description only: none of them is known
# MAGIC on the Monday when the model is used.

# COMMAND ----------

fire_weather = (
    fires.select(
        "fire_id", "cell_id", "week_start", "start_date", "latitude", "longitude", "area_ha", "area_ha_outlier",
        "cause", (F.col("cause") == "Lightning").alias("is_lightning"),
    )
    .join(
        hotspot_features.select("cell_id", "week_start", "year", "month", "week_of_year", "center_lat", "center_lon",
                                "station", "station_km", *WEATHER_FEATURES, *HISTORY_FEATURES),
        ["cell_id", "week_start"],
    )
)

write_table(fire_weather, f"{GOLD}.fire_weather", ["fire_id"],
            "Intensive-zone fires with the features of their cell-week as of Monday (Model 2)")
fire_weather = spark.table(f"{GOLD}.fire_weather")

assert fire_weather.count() == in_panel, "fire_weather must have one row per fire in the hotspot panel"
assert fire_weather.select("fire_id").distinct().count() == in_panel, "fire_id is not unique"

display(fire_weather.agg(
    F.count("*").alias("fires"),
    F.round(F.avg(F.col("is_lightning").cast("int")) * 100, 1).alias("lightning_pct"),
    F.round(F.avg(F.col("station").isNotNull().cast("int")) * 100, 1).alias("matched_pct"),
    F.percentile_approx("station_km", 0.5).alias("median_station_km"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC **Example (spatial + temporal join).** Fire `20221080001` started on Wednesday 2022-04-06 in cell `45.0_-72.5`. Its
# MAGIC features are those of that cell for the week starting Monday 2022-04-04: Richmond's weather up to Sunday April 3.

# COMMAND ----------

display(
    fire_weather.filter(F.col("fire_id") == "20221080001")
    .select("fire_id", "start_date", "week_start", "cause", "cell_id", "station", "station_km",
            "precip_7d_mm", "days_since_rain", "max_temp_7d_mean_c", "windy_days_7d", "fires_prev_52wk")
)
