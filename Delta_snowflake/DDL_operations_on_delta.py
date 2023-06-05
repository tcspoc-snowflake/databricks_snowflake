# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsgen2snowflakepoc2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsgen2snowflakepoc2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsgen2snowflakepoc2.dfs.core.windows.net", "d376db64-7243-47b9-9188-3465248891f6")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsgen2snowflakepoc2.dfs.core.windows.net", "h9-8Q~uSlTpdVRwEPmu.ITB.6pNh_POGjKMnLaoj")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsgen2snowflakepoc2.dfs.core.windows.net", "https://login.microsoftonline.com/3729b040-caed-4a8b-b299-2f736618840e/oauth2/token")

# COMMAND ----------

tablePath= 'abfss://raw-data@adlsgen2snowflakepoc2.dfs.core.windows.net/delta_format'

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

df.write.mode("append").format("delta").saveAsTable("snow_test.DF_customer", path=tablePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC update snow_test.customer set state='QLD'
# MAGIC where state='TAS'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from snow_test.DF_customer where state='TAS';

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from snow_test.DF_customer where state='TAS';
