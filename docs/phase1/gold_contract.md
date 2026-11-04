# Gold Data Contract

What the Gold layer guarantees to the models and the dashboard. Produced by `notebooks/03_gold_features.py` from the
Silver tables (see `docs/phase1/silver_contract.md`). The labels **asserted**, **NOT NULL constraint**, **by construction**
and **verified** have the same meaning as in the Silver contract.

**Scope.** Intensive protection zone only. Nordic-zone fires are a median 399 km from the nearest station, too far for
representative weather; they remain in `silver.fires` for descriptive use.

**Everything is as of Monday morning (by construction).** The models are used each Monday: Model 1 flags the cells likely
to have a fire that week, then Model 2 estimates the likely cause in those cells. Every feature describes the time before
the week's Monday (`week_start`). A week's own weather first appears in the following week's row.

## Tables

| Table | One row = | Primary key | Used by | Coverage |
|---|---|---|---|---|
| `gold.grid_cells` | one 0.5° cell with at least one Intensive-zone fire in 2000–2024 | `cell_id` | dashboard map | 324 cells, 308 with `in_model = true` |
| `gold.hotspot_features` | one `in_model` cell in one week whose Monday is in April–October | (`cell_id`, `week_start`) | Model 1 (hotspot) | 2000–2024 |
| `gold.fire_weather` | one fire with a row in `hotspot_features`, with that row's features | `fire_id` | Model 2 (cause) | 2000–2024 |

## Model columns

Both model tables have the same features. ✓ = the column is in that table.

| Column | Role | Description | `hotspot_features` (Model 1) | `fire_weather` (Model 2) |
|---|---|---|:-:|:-:|
| `fire_id` | key | Fire identifier from `silver.fires` | | ✓ |
| `cell_id` | key | Grid cell, named by its south-west corner (e.g. `45.0_-72.5`) | ✓ | ✓ |
| `week_start` | key | Monday of the week | ✓ | ✓ |
| `year` | split | Year of `week_start`, for the time-based train/validation/test split | ✓ | ✓ |
| `station` | spatial join | Weather station whose data the row uses | ✓ | ✓ |
| `station_km` | spatial join | Distance from the cell centre to `station` (km) | ✓ | ✓ |
| `week_of_year` | feature | ISO week number of `week_start` | ✓ | ✓ |
| `center_lat` | feature | Latitude of the cell centre (degrees) | ✓ | ✓ |
| `center_lon` | feature | Longitude of the cell centre (degrees) | ✓ | ✓ |
| `precip_7d_mm` | feature | Total precipitation over the 7 days before Monday (mm) | ✓ | ✓ |
| `precip_14d_mm` | feature | Total precipitation over the 14 days before Monday (mm) | ✓ | ✓ |
| `precip_30d_mm` | feature | Total precipitation over the 30 days before Monday (mm) | ✓ | ✓ |
| `days_since_rain` | feature | Days since the last day with ≥ 1 mm of precipitation (1 = it rained on Sunday) | ✓ | ✓ |
| `max_temp_7d_mean_c` | feature | Mean daily maximum temperature over the 7 days before Monday (°C) | ✓ | ✓ |
| `windy_days_7d` | feature | Days with a gust ≥ 31 km/h in the 7 days before Monday (0–7) | ✓ | ✓ |
| `fires_prev_4wk` | feature | Fires in the cell in the 4 weeks before this week | ✓ | ✓ |
| `fires_prev_52wk` | feature | Fires in the cell in the 52 weeks before this week | ✓ | ✓ |
| `cell_fire_rate_prior` | feature | Average number of fires per year, from 2000 before this week. It measures how fire-prone the cell is in general | ✓ | ✓ |
| `cell_lightning_share_prior` | feature | Proportion of the cell's earlier fires caused by lightning (0–1) before this week | ✓ | ✓ |
| `has_fire` | **target** | At least one Intensive-zone fire started in the cell that week | ✓ | |
| `is_lightning` | **target** | The fire was caused by lightning (false = human-caused) | | ✓ |

Only the `feature` columns are model inputs.

## `gold.grid_cells`

The list of grid cells the project covers, one row per cell. Québec is divided into 0.5° × 0.5° cells (about 55 km
north-south by 39 km east-west in southern Québec), and every cell with at least one Intensive-zone fire in 2000–2024 is
listed. It is a lookup table, not a model input: the dashboard joins predictions to it by `cell_id` to place each cell on
the map, and `in_model` marks the cells the models cover.

| Column | Description |
|---|---|
| `cell_id` | Grid cell, named by its south-west corner (e.g. `45.0_-72.5`) |
| `cell_lat`, `cell_lon` | South-west corner of the cell (degrees) |
| `center_lat`, `center_lon` | Centre of the cell (degrees) |
| `fires_2000_2024` | Number of Intensive-zone fires in the cell, between 2000 and 2024 |
| `lightning_fires_2000_2024` | Of those count above, number of fires caused by lightning |
| `nearest_station`, `nearest_station_km` | Name of Nearest weather station to the cell centre, and its distance (km) |
| `in_model` | The cell is covered by the models (b/c its nearest station is within 300 km of the cell centre) |

## Rules

| Rule | Backed by |
|---|---|
| A window feature (7, 14 or 30 days before Monday) is null when fewer than 80% of its days have a value in Silver | by construction |
| `days_since_rain` is null when a precipitation gap follows the last rain day | by construction |
| `windy_days_7d` counts a `<31` day as calm; null when fewer than 6 of the 7 days have a gust reading | by construction |
| `station` is the nearest station to the cell centre with core weather (`precip_7d_mm` and `max_temp_7d_mean_c`) on that Monday, within 300 km | by construction |
| History features count earlier weeks only | by construction |
| `cell_id` is non-null and unique in `grid_cells` | NOT NULL constraint, primary key |
| `in_model` = the nearest station to the cell centre is within 300 km; the 16 cells without one (40 fires) are not in the model tables | by construction |
| Exactly one row per `in_model` cell per season week in `hotspot_features`; (`cell_id`, `week_start`) unique | asserted |
| Every Intensive-zone fire in an `in_model` cell whose week starts in April–October is counted in its cell-week | asserted |
| One row per such fire in `fire_weather`; `fire_id` unique | asserted |
| A `fire_weather` row's features are exactly those of its (`cell_id`, `week_start`) row in `hotspot_features` | by construction |

**Models must handle**
- `station` and the weather features are null when no station within 300 km has data on that Monday.
- `windy_days_7d` is null on about 60% of rows (gusts are recorded at some stations only).
- `fires_prev_52wk` and `cell_fire_rate_prior` are null in 2000 (less than a year of history).
- `cell_lightning_share_prior` is null until a cell's first fire.
- Fires whose week starts in late March or early November have no row.
- The cell list uses fires from all years, including the test years (a small, known look-ahead in the choice of cells).
- Split by `year`, not at random: several fires in one cell-week share identical features in `fire_weather`.
