# Silver Data Contract

What the Silver layer guarantees to Gold. Produced by `notebooks/02_silver_cleaning.py`.
For *why* each cleaning decision was made, see the notebook and `silver.cleaning_log`.

**How each guarantee is backed**

| Label | Meaning |
|---|---|
| **asserted** | The notebook fails if the guarantee is broken |
| **NOT NULL constraint** | Enforced by Delta: a write with a null in this column fails |
| **by construction** | The code makes it true (filter, translation, derivation), nothing re-checks it |
| **verified** | Checked on the current data and found to hold; not re-checked on later runs |

Primary and foreign keys are declared in Unity Catalog but are **informational only**: Databricks does not reject
duplicate keys or orphan foreign keys. They document the model; the guarantees come from the asserts and NOT NULL constraints.

## Tables

| Table | One row = | Primary key | Coverage |
|---|---|---|---|
| `silver.fires` | one fire origin point | `fire_id` | 2000–2024 |
| `silver.weather_stations` | one ECCC weather station (30 stations) | `station` | — |
| `silver.weather_daily` | one station on one calendar day | (`station`, `date`) | 2000-01-01 to 2025-12-31, every day |
| `silver.cleaning_log` | one cleaning action and the rows it affected | — | audit trail, not for modelling |

Foreign key: `weather_daily.station → weather_stations.station`

`station` is the ECCC station name in title case (e.g. `Lac aux Sables`). `weather_stations` also keeps the ECCC `climate_id` and the original `eccc_name`.

## `silver.fires`

| Guarantee | Backed by |
|---|---|
| `fire_id` is unique | asserted |
| `fire_id` is non-null | NOT NULL constraint |
| `year` is between 2000 and 2024 and non-null | by construction (filter) |
| No two fires share the same `start_date` + `latitude` + `longitude` (the larger-area record is kept) | by construction |
| `cause` is `Lightning` or `Human` (`Foudre` / `Humaine` translated) | by construction + verified (distinct values) |
| `section` is `Intensive` or `Nordic` (`Nordique` → `Nordic`; `Intensive` is the same word in English) | by construction + verified (distinct values) |
| `area_ha ≥ 0` | verified (§1.3: 0 rows with a negative area) |
| `area_ha = 0` is a real value (a fire under 0.05 ha), not a missing-value code | verified (§1.2 analysis) |
| `start_date`, `report_date`, `extinction_date`, `latitude` and `longitude` have no nulls | verified (§1.2 null profile) |
| `area_ha_outlier = true` when the area is over 200 ha; these fires are kept | by construction |
| Every fire passes all validity rules: reported and extinguished on or after its start date, start date in the fire's `year`, coordinates inside Québec, non-negative area | verified (§1.3) |

**Gold must handle**
- Nordic-zone fires (`section = 'Nordic'`) are out of scope for modelling: they are a median 399 km from the nearest station, too far for representative weather (README, Scope). They stay in this table for descriptive use.

## `silver.weather_stations`

| Guarantee | Backed by |
|---|---|
| `station` is non-null | NOT NULL constraint |
| Each `station` has exactly one `climate_id`, one ECCC name and one set of coordinates | asserted (functional dependency check) |
| `station` and `climate_id` are one-to-one (no two stations share a climate ID, and no two ECCC names map to the same `station`) | asserted |

## `silver.weather_daily`

| Guarantee | Backed by |
|---|---|
| (`station`, `date`) is unique | asserted |
| `station` and `date` are non-null | NOT NULL constraint |
| Every station has every calendar day, so a station's rows ordered by `date` form a complete daily series | asserted |
| Values outside physical limits were set to null: temperature −60 to 45 °C, rain and precipitation 0–300 mm, snow 0–200 cm, snow on ground 0–500 cm | by construction |
| `max_temp_c ≥ min_temp_c` wherever both exist | asserted |
| `mean_temp_c = (max + min) / 2`, within the 0.05 °C of ECCC rounding (ECCC rounds .x5 away from zero before 2018 and toward zero from 2018; filled rows round away from zero); heating/cooling degree days = max(0, 18 − mean) / max(0, mean − 18) | asserted on observed rows, by construction on filled rows |
| `total_precip_mm` filled from rain + snow only at stations where precip = rain + snow holds on every complete row (excludes Quebec/Jean Lesage Intl and Richmond) | asserted |
| `max_temp_c`, `min_temp_c`, `mean_temp_c` and `total_precip_mm` each have a `<column>_source`: `observed`, `fd_derived` (mean from observed max/min), `fd_from_imputed` (mean from imputed max/min), `fd2_derived` (precipitation from rain + snow), `interpolated`, `station_month_mean` or `not_imputed` | by construction |
| `max_temp_outlier`, `min_temp_outlier` and `precip_outlier` are non-null booleans; flagged rows are kept | by construction |
| `spd_of_max_gust_below_31` is a non-null boolean; every non-numeric gust speed in Bronze was `<31` | asserted |

**Gold must handle**
- Gaps longer than 30 days are still null (`<column>_source = not_imputed`).
- Outlier flags were computed before nulls were filled, so a filled value is always `false`.
- The ECCC `*_flag` columns describe the original reading. A row flagged `M` may now hold a filled value; use `<column>_source`, not the flag.
- `heat_deg_days_c` and `cool_deg_days_c` have no `_source` column, so observed and derived values cannot be told apart.
- `total_rain_mm`, `total_snow_cm` and `snow_on_grnd_cm` were not filled and keep their nulls.
- Wind gust (`dir_of_max_gust_10s_deg`, `spd_of_max_gust_km_h`) is recorded at some stations only and was not filled. ECCC stores a gust under 31 km/h as `<31`: in Silver, the speed is null and `spd_of_max_gust_below_31` is true. A null speed with `spd_of_max_gust_below_31 = false` means the gust was not recorded.
- Weather covers 2000–2025, fires 2000–2024. Nine stations have almost no 2025 data yet; this is outside the fire period.
- Les Buissons is missing most of 2013 and 2015 (`not_imputed`). On those dates, fires nearest to it need another station or stay null.
- With 30 stations, the median fire is 49 km from its nearest station (45 km in the Intensive zone).
