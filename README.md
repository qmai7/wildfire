# Project Proposal: Forecasting Wildfire Hotspots and Cause in Quebec

**Author:** Quan
**Course:** COMP333 — Data Analytics
**Platform:** Databricks Free Edition + Power BI

---

## 1. Problem Statement and Motivation

Wildfires have become one of Quebec's most consequential recurring natural hazards, with recent fire seasons (notably 2023) causing record-breaking burned area, evacuations, and air-quality impacts far beyond the fire zones themselves. The province's forest protection agency (SOPFEU) maintains a detailed historical record of every fire's origin point, cause, and dates, but — like most operational logs — this data describes *what already happened*, not *where risk is concentrated going forward*.

This project addresses that gap by building an end-to-end data engineering and machine learning pipeline that transforms raw wildfire origin records, combined with historical weather data, into two things a decision-maker could act on: (1) a forecast of which regions and time periods are likely to become fire **hotspots**, and (2) conditional on a hotspot being likely, a prediction of the probable **cause** (human activity vs. lightning) — since human-caused and lightning-caused fires call for different prevention responses (public awareness/access restrictions vs. detection/suppression readiness). Beyond the civil-protection framing, the project demonstrates the full modern data stack — multi-source data integration, governed storage, transformation, feature engineering, model training and tracking, and automated batch deployment — using a single, cloud-first platform.

## 2. Datasets

This project integrates **two independently sourced datasets**, which is a core requirement of the assignment and a natural fit here since neither dataset alone is sufficient for meaningful forecasting.

**1. Wildfire origin points — SOPFEU / Ministère des Ressources naturelles et des Forêts (Données Québec, CC-BY).** Point-level records of every wildfire in Quebec since 1972, including start/report/extinction dates, cause (Humaine / Foudre), burned area (hectares), and precise coordinates. Scoped to **2000–2024** for this project, giving **15,078 fire records** — enough volume for robust time-series and spatial modeling while keeping scope manageable.

**2. Historical daily weather — Environment and Climate Change Canada.** Daily temperature (max/min/mean) and precipitation data from three representative weather stations, selected to cover the three regions where fire activity is most concentrated in the filtered dataset:
- **Sorel** (southern Quebec — Montérégie/Mauricie fire-dense band)
- **Lac Bouchette** (Saguenay–Lac-Saint-Jean / Abitibi boreal region)
- **Les Buissons** (Côte-Nord)

## 3. Objectives

1. Build a governed, layered data pipeline (Bronze → Silver → Gold) that ingests and integrates fire and weather data from two independent sources into a model-ready feature set.
2. Train a model that forecasts wildfire **hotspot risk** across a spatial grid and time-bin structure covering Quebec's fire-active regions.
3. Train a second model that predicts the most likely **cause** (human vs. lightning) conditional on a location/time being flagged as a hotspot.
4. Operationalize both models through **scheduled batch scoring**, rather than a live endpoint, to demonstrate a realistic, low-maintenance MLOps pattern appropriate for this project's scope.
5. Deliver results through an interactive **Power BI dashboard** for exploration and presentation.

## 4. Methodology

**Data Integration.** This is a central challenge of the project, addressed through two distinct join operations: a **spatial join**, assigning each fire record to its nearest weather station using haversine distance (validated on a 113-record test subset with a 100% match rate); and a **temporal join**, merging each fire's start date against that station's daily weather record. The fire data (Shapefile) and weather data (CSV, one file per station per year) come from entirely different agencies, formats, and schemas, requiring genuine reconciliation rather than a trivial key match.

**Data Cleaning and Transformation.** The Silver layer standardizes both sources: parsing and validating fire dates and coordinates, normalizing weather column names/units, and handling missing values in optional weather fields (e.g., wind gust data, which is sparsely populated).

**Feature Engineering.** The Gold layer aggregates integrated fire-weather records into a spatial grid × time-bin structure (e.g., region and month/week). For each grid-cell/time-bin, historical fire counts, weather conditions (temperature, precipitation) from the nearest station, day-of-year/season, and cause mix are computed, producing the feature table used for modeling.

**Modeling.** A two-stage approach is used, mirroring the integration structure. 

- **Model 1** is a **binary classification model** that predicts the probability of at least one fire occurring in a given grid-cell/time-bin, using weather and historical fire features; a cell is flagged as a hotspot when its predicted probability exceeds a chosen threshold (e.g., top decile of risk).

- **Model 2**, trained only on cells flagged as hotspots, is a second **binary classification model** predicting the most likely cause (human vs. lightning) given the same features — a meaningful distinction since dry, low-precipitation conditions favor lightning ignition while human-caused fires correlate more with access/proximity patterns. All experiments and resulting models are tracked with **MLflow**, with a promoted model designated "Production" in the Model Registry.

**Deployment via Batch Scoring.** Rather than a real-time API endpoint, a scheduled Databricks Job periodically loads the current Production model, scores the latest Gold-layer data, and writes results to a Predictions Delta table. This is a standard, low-overhead production pattern and avoids the need for a live serving endpoint, custom application, or container orchestration — appropriate given this project's cloud-first, infrastructure-light scope.

**Visualization.** Power BI connects directly to the Databricks SQL Warehouse to read Gold and Predictions tables, presenting risk maps by region, cause breakdowns, and seasonal trend views, refreshed on the same cadence as the scoring job.

## 5. Technology Stack

| Layer | Tool |
|---|---|
| Platform | Databricks Free Edition |
| Storage | Delta Lake (Bronze / Silver / Gold / Predictions) |
| Governance | Unity Catalog |
| Compute | Serverless notebooks (PySpark / Pandas / GeoPandas for shapefile ingestion) |
| Modeling | scikit-learn, tracked via MLflow |
| Orchestration | Databricks Jobs (scheduled batch scoring) |
| Visualization | Power BI (via Databricks SQL Warehouse) |

## 6. Expected Deliverables

- A documented, layered Delta Lake pipeline (Bronze/Silver/Gold/Predictions) that integrates fire and weather data, registered in Unity Catalog.
- Two trained and MLflow-tracked models: wildfire hotspot forecasting and conditional cause classification.
- A scheduled batch-scoring job demonstrating an automated, reproducible deployment pattern.
- An interactive Power BI dashboard presenting the forecast results.
- A final report documenting design decisions, the data integration approach and challenges encountered (schema/format reconciliation, spatial + temporal joins), evaluation results, and how the project addresses data integration, cleaning/transformation, visualization, and modeling/deployment challenges.

## 7. Evaluation Approach

Model quality will be assessed using a **time-based train/validation/test split** (to avoid leakage from future data), with accuracy, precision/recall, and AUC for hotspot classification (fire vs. no-fire probability), and accuracy/F1-score for cause classification (human vs. lightning). Data integration quality will be assessed by the join match rate (proportion of fire records successfully matched to weather data) and the completeness and correctness of the Bronze-to-Predictions lineage within Unity Catalog.

## 8. Conclusion

This project demonstrates a realistic, cloud-first application of the full data engineering and MLOps stack — from two independently sourced open-government datasets, through genuine spatial and temporal data integration, governed transformation layers, feature engineering, model training and tracking, and automated batch deployment — applied to a timely and consequential civic problem for Quebec. Its two-stage modeling design, multi-source integration, and batch-scoring deployment strategy are scoped to be achievable within a course timeline while still reflecting production-realistic engineering practices.