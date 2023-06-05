# Databricks notebook source
 df1 = spark.read.format('csv').option("header", "true")\
               .option("inferSchema", "true")\
                .option("delimiter","|")\
               .load("dbfs:/FileStore/tables/customer.csv")
display(df1)

# COMMAND ----------

 options = {
    "sfUrl": "sxmyhrr-gx33748.snowflakecomputing.com",
    "sfUser": "NINGU07",
    "sfPassword": "Ningu@123",
    "sfDatabase": "CRM_DATA",
    "sfSchema": "SCH_LANDING",
    "sfWarehouse": "COMPUTE_WH",
    }

# COMMAND ----------

spark.read.format("csv").option("header","true").option("delimiter","|").load("dbfs:/FileStore/tables/customer.csv").show()

# COMMAND ----------

spark.read.format("csv").option("header","true").option("delimiter","|").load("dbfs:/FileStore/tables/customer.csv").write.format("snowflake").options(**options).mode("overwrite").option("dbtable","CUSTOMER").save()
