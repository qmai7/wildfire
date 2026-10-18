# Gold Data Contract

What the Gold layer guarantees to the models and the dashboard. Produced by `notebooks/03_gold_features.py` from the
Silver tables (see `docs/phase1/silver_contract.md`). The labels **asserted**, **NOT NULL constraint**, **by construction**
and **verified** have the same meaning as in the Silver contract.

**Scope.** Intensive protection zone only. Nordic-zone fires are a median 399 km from the nearest station, too far for
representative weather; they remain in `silver.fires` for descriptive use.

**Everything is as of Monday morning (by construction).** The models are used each Monday: Model 1 flags the cells likely
to have a fire that week, then Model 2 estimates the likely cause in those cells. Every weather and fire-history feature
describes the time before the week's Monday (`week_start`). A week's own weather first appears in the following week's row.
Model 2 is trained on the same Monday features, since no fire has started when it is used.

## Tables

| Table | One row = | Primary key | Coverage |
|---|---|---|---|
| `gold.grid_cells` | one 0.5° cell with at least one Intensive-zone fire in 2000–2024 | `cell_id` | 324 cells, 308 with `in_model = true` |
| `gold.hotspot_features` | one `in_model` cell in one week whose Monday is in April–October | (`cell_id`, `week_start`) | 2000–2024 |
| `gold.fire_weather` | one fire with a row in `hotspot_features`, with that row's features | `fire_id` | 2000–2024 |

## Shared rules

| Rule | Backed by |
|---|---|
| A window feature (7, 14 or 30 days before Monday) is null when fewer than 80% of its days have a value in Silver | by construction |
| `days_since_rain` counts days since the last day with ≥ 1 mm (1 = it rained on Sunday); null when a precipitation gap follows the last rain day | by construction |
| `windy_days_7d` counts days with a gust ≥ 31 km/h; a `<31` day counts as calm; null when fewer than 6 of the 7 days have a gust reading | by construction |
| `*_observed_share` is the share of the window's days whose value was observed in Silver, not imputed | by construction |
| `station` is the nearest station to the cell centre with core weather (`precip_7d_mm` and `max_temp_7d_mean_c`) on that Monday, within 300 km | by construction |
| `cell_id` is the cell's south-west corner, `<lat>_<lon>` with one decimal (e.g. `45.0_-72.5`) | by construction |

## `gold.grid_cells`

| Guarantee | Backed by |
|---|---|
| `cell_id` is non-null | NOT NULL constraint |
| `in_model` = the nearest station to the cell centre is within 300 km; the 16 cells without one (40 fires) have no representative weather and are left out of both model tables | by construction |

## `gold.hotspot_features`

| Guarantee | Backed by |
|---|---|
| Exactly one row per `in_model` cell per season week; (`cell_id`, `week_start`) unique | asserted |
| `fire_count` sums to the number of Intensive-zone fires in `in_model` cells whose week starts in April–October | asserted |
| `has_fire` = `fire_count` > 0 | by construction |

**Models must handle**
- `fires_prev_52wk` and `cell_fire_rate_prior` are null in 2000 (less than a year of history).
- `cell_lightning_share_prior` is null until a cell's first fire.
- `windy_days_7d` is null on about 60% of rows (gusts are recorded at some stations only).
- Fires whose week starts in late March or early November have no row (their week is outside April–October).
- The cell list uses fires from all years, including the test years. Almost every cell has fires in many years, so the effect is small, but it is a known look-ahead in the choice of cells.
- `fire_count`, `lightning_count` and `human_count` describe the row's own week: they are labels, never features.

## `gold.fire_weather`

| Guarantee | Backed by |
|---|---|
| One row per fire counted in `hotspot_features`; `fire_id` unique | asserted |
| The features are exactly those of the fire's (`cell_id`, `week_start`) row in `hotspot_features` | by construction |
| `is_lightning` = (`cause` = `Lightning`) | by construction |

**Models must handle:** `start_date`, `latitude`, `longitude`, `area_ha` and `area_ha_outlier` describe the fire itself and
are not known on the Monday when Model 2 is used. They are for description only, never features.
