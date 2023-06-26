# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up the environment.

# COMMAND ----------

# MAGIC %md
# MAGIC * You must have a Confluent cluster, an API Key and secret, a Schema Registry, an API Key and secret for the registry, and a topic
# MAGIC * Do a pip install of this: confluent-kafka[avro,json,protobuf]>=1.4.2 OR install the following two wheel files: 
# MAGIC * -------- fastavro 1.1.0 - https://pypi.org/project/fastavro/#files, file name fastavro-1.1.0-cp37-cp37m-manylinux2014_x86_64.whl
# MAGIC * -------- confluent-kafka 1.5.0 - https://pypi.org/project/confluent-kafka/#files, file name confluent_kafka-1.5.0-cp37-cp37m-manylinux1_x86_64.whl
# MAGIC * Notebooks must be detached and re-attached before they can see new libraries
# MAGIC * For production use you'll need the pip install in an init script or pre-load the files as libraries into the Workspace and then reference in Cluster setup

# COMMAND ----------

confluentClusterName = "avroTest"
confluentBootstrapServers = "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
confluentApiKey = dbutils.secrets.get(scope = "confluentTest", key = "api-key")
confluentSecret = dbutils.secrets.get(scope = "confluentTest", key = "secret")
confluentRegistryApiKey = dbutils.secrets.get(scope = "confluentTest", key = "registry-api-key")
confluentRegistrySecret = dbutils.secrets.get(scope = "confluentTest", key = "registry-secret")
confluentTopicName = "blogTest"
schemaRegistryUrl = "https://psrc-4r3xk.us-east-2.aws.confluent.cloud"
deltaTablePath = "abfss://streamingtest@achuadlsgen2test.dfs.core.windows.net/delta/clickstreamTest"
checkpointPath = "abfss://streamingtest@achuadlsgen2test.dfs.core.windows.net/delta/checkpoints/clickstreamTest"

# COMMAND ----------

# Grabbing my storage key for reading and writing below
# If you have mounted storage to DBFS then refer to those paths below and you can skip this step
adlsGen2Key = dbutils.secrets.get(scope = "confluentTest", key = "adlsgen2-key")
spark.conf.set("fs.azure.account.key.achuadlsgen2test.dfs.core.windows.net", adlsGen2Key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the client for the Schema Registry

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient
import ssl

schema_registry_conf = {
    'url': schemaRegistryUrl,
    'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method to read the schema each time you write - safer for schemas that will evolve and required for topics that have multiple types of data coming through them

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the Readstream

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
clickstreamTestDf = (
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
  .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','fixedValue')
)

# COMMAND ----------

# This is to give the user a view of the dataset being fed into the foreachBatch function as part of the writeStream.  Remove this cell before executing this Notebook as a job.
display(clickstreamTestDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out data from the stream using foreachBatch

# COMMAND ----------

# This method will pull the set of schemas needed to process each micro-batch.  It will pull the schemas each time a micro-batch is processed.
# This example will handle parsing rows with different schemas - it uses the schema ID to pull the correct schema for the value for each set of rows
# In this example the key is a string.  Keys can also be encoded in Avro format - in this case the below can be turned into a nested for loop where the
# schemas for the keys and values can be pulled from the registry and parsed

# To extend this further, you can use another attribute of each row to filter on so that different types of data that came in on the same topic can be identified, then 
# after parsing write each set of data out to the correct Delta table
# You can also manipulate and transform the data more before writing it out.  
# In any case it is prudent to write a form of the data out that is as raw as possible to facilitate reprocessing.

import pyspark.sql.functions as fn
from pyspark.sql.avro.functions import from_avro

def parseAvroDataWithSchemaId(df, ephoch_id):
  # Cache this so that when the logic below uses the Dataframe more than once the data is not pulled from the topic again
  cachedDf = df.cache()
  
  # Set the option for what to do with corrupt data in from_avro - either stop on the first failure it finds (FAILFAST) or just set corrupt data to null (PERMISSIVE)
  fromAvroOptions = {"mode":"FAILFAST"}
  #fromAvroOptions= ("mode":"PERMISSIVE")
  
  # Function that will fetch a schema from the schema registry by ID
  def getSchema(id):
    return str(schema_registry_client.get_schema(id).schema_str)

  # Fetch the distinct set of value Schema IDs that are in this micro-batch
  distinctValueSchemaIdDF = cachedDf.select(fn.col('valueSchemaId').cast('integer')).distinct()

  # For each valueSchemaId get the schemas from the schema registry
  for valueRow in distinctValueSchemaIdDF.collect():
    # Pull the schema for this schema ID
    currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
    currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))
    
    # Filter the batch to the rows that have this value schema ID
    filterValueDF = cachedDf.filter(fn.col('valueSchemaId') == currentValueSchemaId.value)
    
    # Then for all the rows in the dataset with that value schema ID, parse the Avro data with the correct schema and write the micro-batch to a Delta table
    # In this example mergeSchema is set to true to enable schema evolution.  Set to false (which is the default) to prevent schema changes on the Delta table
    filterValueDF \
      .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', from_avro('fixedValue', currentValueSchema.value, fromAvroOptions).alias('parsedValue')) \
      .write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .save(deltaTablePath)

# COMMAND ----------

# Add this to the statement below to just trigger the writeStream once
#.trigger(once=True) \

# Write out the parsed data to a Delta table
clickstreamTestDf.writeStream \
  .option("checkpointLocation", checkpointPath) \
  .foreachBatch(parseAvroDataWithSchemaId) \
  .queryName("clickStreamTestFromConfluent") \
  .start()

# COMMAND ----------

# Read the data from the Delta table
deltaClickstreamTestDf = spark.read.format("delta").load(deltaTablePath)
display(deltaClickstreamTestDf)

# COMMAND ----------


