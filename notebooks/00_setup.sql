-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Setup
-- MAGIC
-- MAGIC Creates the Unity Catalog structure for the project. Safe to re-run: every statement uses `IF NOT EXISTS`.
-- MAGIC
-- MAGIC After running, upload `Feux_pt_ori_SHP.zip` and `weather_2000_2025.zip` to the `raw_files` volume, then run `01_bronze_ingestion`.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS wildfire_project;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS wildfire_project.bronze COMMENT 'landing zone';
CREATE SCHEMA IF NOT EXISTS wildfire_project.silver COMMENT 'data processing + cleaning + type casting';
CREATE SCHEMA IF NOT EXISTS wildfire_project.gold;
CREATE SCHEMA IF NOT EXISTS wildfire_project.predictions;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS wildfire_project.bronze.raw_files;
