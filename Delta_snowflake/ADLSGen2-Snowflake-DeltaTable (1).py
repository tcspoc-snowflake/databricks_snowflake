# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsgen2snowflakepoc2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsgen2snowflakepoc2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsgen2snowflakepoc2.dfs.core.windows.net", "d376db64-7243-47b9-9188-3465248891f6")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsgen2snowflakepoc2.dfs.core.windows.net", "h9-8Q~uSlTpdVRwEPmu.ITB.6pNh_POGjKMnLaoj")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsgen2snowflakepoc2.dfs.core.windows.net", "https://login.microsoftonline.com/3729b040-caed-4a8b-b299-2f736618840e/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw-data@adlsgen2snowflakepoc2.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE snow_test.CUSTOMER(
# MAGIC ID STRING,
# MAGIC Name STRING,
# MAGIC Address STRING,
# MAGIC City STRING,
# MAGIC PostCode INT,
# MAGIC State STRING,
# MAGIC Company STRING,
# MAGIC Contact STRING
# MAGIC )
# MAGIC
# MAGIC USING DELTA
# MAGIC
# MAGIC LOCATION 'abfss://raw-data@adlsgen2snowflakepoc2.dfs.core.windows.net/';

# COMMAND ----------

tablePath = 'abfss://raw-data@adlsgen2snowflakepoc2.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from snow_test.CUSTOMER

# COMMAND ----------

# File location and type
file_location = "abfss://staging@adlsgen2snowflakepoc2.dfs.core.windows.net/customer.csv"
file_type = "csv"
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "|"
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("snow_test.CUSTOMER", path=tablePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from snow_test.customer where state='VIC';

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from snow_test.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC update snow_test.customer set state='TAS'
# MAGIC where state='QLD'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --Generate manifests of a Delta table using Apache Spark
# MAGIC
# MAGIC GENERATE symlink_format_manifest FOR TABLE snow_test.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC GENERATE symlink_format_manifest FOR TABLE delta.`abfss://raw-data@adlsgen2snowflakepoc2.dfs.core.windows.net/`

# COMMAND ----------


