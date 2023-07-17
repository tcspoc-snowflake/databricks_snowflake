# Databricks notebook source
mount_name_data = "abfss://dlt@adlspocwork.dfs.core.windows.net/landing/"
mount_name_schema = "abfss://dlt@adlspocwork.dfs.core.windows.net/schema_drift/"

# COMMAND ----------

import dlt

@dlt.table(
      comment="Raw Data feed for ZipCode"
)
def zipcode_raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation",mount_name_schema)
      .load(mount_name_data)
  )
