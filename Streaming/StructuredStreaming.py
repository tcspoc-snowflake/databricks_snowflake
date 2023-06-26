# Databricks notebook source
confluentClusterName = "tcspocwork"
confluentBootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
confluentApiKey = "5ALKISGBPKABNLBM"
confluentSecret = "kl0oNGie6vmuupATR1IHhpwBYJtT5hWLTL2fpFl57brjYdIw/gqZh1lWLD9t0fT7"
confluentRegistryApiKey = "PNORSKM6HAGVKYDV"
confluentRegistrySecret = "R0WANqpOYr2WljSpeRFA8utN2UnUOMMDusXLLb7z4hiHOl9Sxi5XS6Mq6gUmkIs4"
confluentTopicName = "sample_data"
schemaRegistryUrl = "https://psrc-8qyy0.eastus2.azure.confluent.cloud"
deltaTablePath = "abfss://streamingdata@adlspocwork.dfs.core.windows.net/delta/salesorders"
checkpointPath = "abfss://streamingdata@adlspocwork.dfs.core.windows.net/delta/checkpoints/salesorders"

# Grabbing my storage key for reading and writing below
# If you have mounted storage to DBFS then refer to those paths below and you can skip this step
adlsGen2Key = "S/cl4cXxMm8/UDlP67lKW5V01tDTQuI0lSaA2Dw//QEEFklpPEzHq2e4JTG7HMnJ+GG0S1YnwKl9+AStfIsa8Q=="
spark.conf.set("fs.azure.account.key.adlspocwork.dfs.core.windows.net", adlsGen2Key)

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient
import ssl

schema_registry_conf = {
    'url': schemaRegistryUrl,
    'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

# UDF that will decode the magic byte and schema identifier at the front of the Avro data
# Initially binary_to_int was being used.  However for some reason the value that was returned was not
# being interpreted by a distinct() call correctly in the foreachBatch function.  Changing this
# value to a string and casting it to an int later enabled distinct() to get the set of schema IDs
#binary_to_int = fn.udf(lambda x: int.from_bytes(x, byteorder='big'), IntegerType())
binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

# Set up the Readstream, include the authentication to Confluent Cloud for the Kafka topic.
# Note the specific kafka.sasl.jaas.config value - on Databricks you have to use kafkashaded.org... for that setting or else it will not find the PlainLoginModule
# The below is pulling from only one topic, but can be configured to pull from multiple with a comma-delimited set of topic names in the "subscribe" option
# The below is also starting from a specific offset in the topic.  You can specify both starting and ending offsets.  If not specified then "latest" is the default for streaming.
# The full syntax for the "startingOffsets" and "endingOffsets" options are to specify an offset per topic per partition.  
# Examples: 
#    .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")  The -2 means "earliest" and -1 means "latest"
#    .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")  The -1 means "latest", -2 not allowed for endingOffsets
salesordersDf = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load()
  .withColumn('key', fn.col("key").cast(StringType()))
  .withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)"))
  .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','fixedValue','value')
)

# COMMAND ----------

display(salesordersDf)

# COMMAND ----------

import pyspark.sql.types as t
from pyspark.sql.functions import from_json
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

df = (spark.readStream
  .format("kafka")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092")
  .option("kafka.sasl.jaas.config",'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{}" password="{}";'.format("5ALKISGBPKABNLBM", "kl0oNGie6vmuupATR1IHhpwBYJtT5hWLTL2fpFl57brjYdIw/gqZh1lWLD9t0fT7"))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", "sample_data")
  .option("startingOffsets", "latest")
  .load()
  .withColumn('key', fn.col("key").cast(StringType()))
  .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','value')
)

# COMMAND ----------

df.selectExpr('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'valueSchemaId',"CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("memory").queryName("test").start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.functions import from_json
def parseJsonDataWithSchemaId(df, ephoch_id):
  cachedDf = df.cache()
  fromJsonOptions = {"mode":"FAILFAST"}
  def getSchema(id):
    return str(schema_registry_client.get_schema(id).schema_str)
  distinctValueSchemaIdDF = cachedDf.select(fn.col('valueSchemaId').cast('integer')).distinct()
  for valueRow in distinctValueSchemaIdDF.collect():
    currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
    currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))
    filterValueDF = cachedDf.filter(fn.col('valueSchemaId') == currentValueSchemaId.value)
    filterValueDF \
      .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', from_json('Value', currentValueSchema.value, fromJsonOptions).alias('parsedValue')) \
      .write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
     .save(deltaTablePath)

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.functions import from_json
def parseJsonDataWithSchemaId(df, ephoch_id):
  cachedDf = df.cache()
  fromJsonOptions = {"mode":"FAILFAST"}
  #def getSchema(id):
  #  return str(schema_registry_client.get_schema(id).schema_str)
  #distinctValueSchemaIdDF = cachedDf.select(fn.col('valueSchemaId').cast('integer')).distinct()
  #for valueRow in distinctValueSchemaIdDF.collect():
    #currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
    #currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))
    #filterValueDF = cachedDf.filter(fn.col('valueSchemaId') == currentValueSchemaId.value)
    #filterValueDF \
  df.select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'value') \
      .write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
     .save(deltaTablePath)

# COMMAND ----------

df.writeStream \
  .option("checkpointLocation", checkpointPath) \
  .foreachBatch(parseJsonDataWithSchemaId) \
  .queryName("OrdersFromConfluent") \
  .start()

# COMMAND ----------

OrdersFromConfluent = spark.read.format("delta").load(deltaTablePath)
display(OrdersFromConfluent)
