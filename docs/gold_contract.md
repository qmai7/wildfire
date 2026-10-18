# Gold Data Contract

What the Gold layer guarantees to the models and the dashboard. Produced by `notebooks/03_gold_features.py` from the
Silver tables (see `docs/phase1/silver_contract.md`). The labels **asserted**, **NOT NULL constraint**, **by construction**
and **verified** have the same meaning as in the Silver contract.

**Scope.** Intensive protection zone only. Nordic-zone fires are a median 399 km from the nearest station, too far for
representative weather; they remain in `silver.fires` for descriptive use.

**No look-ahead (by construction).** Every weather and fire-history feature is computed from days strictly before the
row's reference date: the fire's `start_date` in `fire_weather`, the Monday `week_start` in `hotspot_features`. A row's
own period only appears in its label.

## Tables

| Table | One row = | Primary key | Coverage |
|---|---|---|---|
| `gold.weather_lookback` | one station on one day, describing the days before it | (`station`, `date`) | 30 stations, 2000-01-01 to 2025-12-31 |
| `gold.grid_cells` | one 0.5° cell with at least one Intensive-zone fire in 2000–2024 | `cell_id` | 324 cells, 308 with `in_model = true` |
| `gold.fire_weather` | one Intensive-zone fire | `fire_id` | 2000–2024 |
| `gold.hotspot_features` | one cell in one week whose Monday is in April–October | (`cell_id`, `week_start`) | 2000–2024 |

## Shared rules

| Rule | Backed by |
|---|---|
| A window feature (7, 14 or 30 days) is null when fewer than 80% of its days have a value in Silver | by construction |
| `days_since_rain` counts days since the last day with ≥ 1 mm (1 = the day before); null when a precipitation gap follows the last rain day | by construction |
| `windy_days_7d` counts days with a gust ≥ 31 km/h; a `<31` day counts as calm; null when fewer than 6 of the 7 days have a gust reading | by construction |
| `*_observed_share` is the share of the window's days whose value was observed in Silver, not imputed | by construction |
| The station used for a point and date is the nearest one with `has_weather` on that date, within 300 km; `nearest_station` is the nearest overall | by construction |
| `cell_id` is the cell's south-west corner, `<lat>_<lon>` with one decimal (e.g. `45.0_-72.5`) | by construction |

## `gold.weather_lookback`

| Guarantee | Backed by |
|---|---|
| (`station`, `date`) is non-null | NOT NULL constraint |
| `has_weather` = `precip_7d_mm` and `max_temp_7d_mean_c` are both non-null | by construction |

## `gold.grid_cells`

| Guarantee | Backed by |
|---|---|
| `in_model` = the nearest station to the cell centre is within 300 km; cells without one (16 remote cells, 40 fires) have no representative weather and are left out of `hotspot_features` | by construction |

## `gold.fire_weather`

| Guarantee | Backed by |
|---|---|
| One row per Intensive-zone fire in `silver.fires`; `fire_id` unique | asserted |
| `is_lightning` = (`cause` = `Lightning`) | by construction |
| `used_fallback_station` is true when the station used is not the nearest one | by construction |
| Cell history features are as of the fire's `week_start` (earlier weeks only) | by construction |

**Models must handle:** weather features are null for fires with no station with data within 300 km, and on windows
below 80% coverage. `windy_days_7d` is often null (gusts are recorded at some stations only).

## `gold.hotspot_features`

| Guarantee | Backed by |
|---|---|
| Exactly one row per `in_model` cell per season week; (`cell_id`, `week_start`) unique | asserted |
| `fire_count` sums to the number of Intensive-zone fires whose week starts in April–October | asserted |
| `has_fire` = `fire_count` > 0 | by construction |
| Weather is from the nearest station with data as of the Monday (the days up to the Sunday before) | by construction |

**Models must handle**
- `fires_prev_52wk` and `cell_fire_rate_prior` are null in 2000 (less than a year of history).
- `cell_lightning_share_prior` is null until a cell's first fire.
- Fires whose week starts in late March or early November have no row (their week is outside April–October).
- The cell list uses fires from all years, including the test years. Almost every cell has fires in many years, so the effect is small, but it is a known look-ahead in the choice of cells.
- `fire_count`, `lightning_count` and `human_count` describe the row's own week: they are labels, never features.
