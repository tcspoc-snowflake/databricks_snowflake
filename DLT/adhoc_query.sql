-- Databricks notebook source
select * from dltpoc.sales_orders_cleaned;

-- COMMAND ----------

select * from dltpoc.sales_order_in_chicago;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.auth.type.adlspocwork.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.adlspocwork.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.adlspocwork.dfs.core.windows.net",'3a542803-6dad-4daa-a048-2de0496ca437')
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.adlspocwork.dfs.core.windows.net", 'GUg8Q~IqSQzlZGX.qg6mbMO3dR5mQV78lF8lQczQ')
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlspocwork.dfs.core.windows.net", "https://login.microsoftonline.com/ff632cb5-1455-42a9-88e1-3922203e5bbf/oauth2/token")

-- COMMAND ----------

select * from zipcode.zipcode_raw;
