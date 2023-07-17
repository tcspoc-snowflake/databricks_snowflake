# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlspocwork.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlspocwork.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlspocwork.dfs.core.windows.net",'3a542803-6dad-4daa-a048-2de0496ca437')
spark.conf.set("fs.azure.account.oauth2.client.secret.adlspocwork.dfs.core.windows.net", 'GUg8Q~IqSQzlZGX.qg6mbMO3dR5mQV78lF8lQczQ')
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlspocwork.dfs.core.windows.net", "https://login.microsoftonline.com/ff632cb5-1455-42a9-88e1-3922203e5bbf/oauth2/token")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": '3a542803-6dad-4daa-a048-2de0496ca437',
           "fs.azure.account.oauth2.client.secret": 'GUg8Q~IqSQzlZGX.qg6mbMO3dR5mQV78lF8lQczQ',
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ff632cb5-1455-42a9-88e1-3922203e5bbf/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://dlt@adlspocwork.dfs.core.windows.net/landing/",
  mount_point = "/mnt/landing/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://dlt@adlspocwork.dfs.core.windows.net/checkpoint/",
  mount_point = "/mnt/checkpoint/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/landing/")

# COMMAND ----------

dbutils.fs.ls("/mnt/checkpoint/")

# COMMAND ----------

files_path = '/mnt/landing/'

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json") 
    .option("cloudFiles.schemaLocation","/mnt/checkpoint/")
    .load(files_path)
)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, count

count_df = (df.withColumn("file", input_file_name()) 
            .groupBy("file") 
            .agg(count("*")) )
display(count_df)

# COMMAND ----------

dbutils.fs.ls("abfss://dlt@adlspocwork.dfs.core.windows.net/landing")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": '3a542803-6dad-4daa-a048-2de0496ca437',
           "fs.azure.account.oauth2.client.secret": 'GUg8Q~IqSQzlZGX.qg6mbMO3dR5mQV78lF8lQczQ',
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ff632cb5-1455-42a9-88e1-3922203e5bbf/oauth2/token"}

mount_name_data = "/mnt/landing/"
mount_name_schema = "/mnt/schema_drift/"

if all(mount.mountPoint != mount_name_data for mount in dbutils.fs.mounts()):
     dbutils.fs.mount(
         source = "abfss://dlt@adlspocwork.dfs.core.windows.net/landing/",
         mount_point = mount_name_data,
         extra_configs = configs)


if all(mount.mountPoint != mount_name_schema for mount in dbutils.fs.mounts()):
     dbutils.fs.mount(
         source = "abfss://dlt@adlspocwork.dfs.core.windows.net/schema_drift/",
         mount_point = mount_name_schema,
         extra_configs = configs)

# COMMAND ----------


