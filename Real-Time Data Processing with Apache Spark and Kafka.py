# Databricks notebook source
# MAGIC %md # Structured Streaming & Basic Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Structured Streaming Overview
# MAGIC <br>
# MAGIC Structured Streaming is a powerful and scalable stream processing engine built on the foundation of the Spark SQL engine. It offers an unified approach to processing both real-time and batch data using the same programming model.
# MAGIC
# MAGIC Structured Streaming queries are internally processed using a default **micro-batch** processing engine, treating data streams as small batch jobs to achieve end-to-end latencies as low as 100 milliseconds and exact-once fault-tolerance.
# MAGIC
# MAGIC Spark jobs are like queries running on top of the **input table** (e.g. a kafka topic) and will update/append rows to the **result table** in a certain interval (e.g. 1 second) if new rows get appended to the input table. We can consider it as an **unbounded table**.
# MAGIC

# COMMAND ----------

# MAGIC %md ## Example of a Consumer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

confluent_username = dbutils.secrets.get(scope="confluent", key="username")
confluent_password = dbutils.secrets.get(scope="confluent", key="password")

spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)


# COMMAND ----------

kafka_consumer_properties = {
    "kafka.bootstrap.servers": "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_username}" password="{confluent_password}";',
    "kafka.sasl.mechanism": "PLAIN",
    "subscribe": "clickstream",
    "startingOffsets": "latest", # earliest
    "failOnDataLoss": "false",
    "kafka.group.id": "group-1",
}

df_read = (
    spark
    .readStream
    .format("kafka")
    .options(**kafka_consumer_properties)
    .load()
)

df_read.display()

# COMMAND ----------

# MAGIC %md ## Basic Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **Narrow Transformations**: No shuffle, every output partition depends on a single input partition.
# MAGIC - **Wide Transformations**: Data needs to be shuffled, it applies for functions that require joins and aggregations.

# COMMAND ----------

# MAGIC %md ### Select

# COMMAND ----------

json_schema = "user_id INT, product_id INT, action STRING, event_timestamp STRING"

df_select = (
    df_read
    .select(from_json(col("value").cast("string"), json_schema).alias("json_data"))
    .select("json_data.*")
)

df_select.display()

# COMMAND ----------

# MAGIC %md ### Filter

# COMMAND ----------

df_filter = df_select.filter(col("user_id") != "1")

df_filter.display()

# COMMAND ----------

# MAGIC %md ### WithColum

# COMMAND ----------

df_transformed = (
    df_filter
    .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
    .withColumn("minute", minute(col("event_timestamp")))
    .withColumn("second", second(col("event_timestamp")))
    .withColumn("action", upper(col("action")))
)

df_transformed.display()

# COMMAND ----------

# MAGIC %md # ForeachBatch, Triggers & Output Modes

# COMMAND ----------

# MAGIC %md #### ForeachBatch

# COMMAND ----------

# MAGIC %md **foreachbatch** is a powerful function available in Spark Structured Streaming that allows you to apply custom processing logic on the micro-batches of data that the streaming job processes. There are some benefits of using foreachBatch:
# MAGIC <br></br>
# MAGIC - Reuse **existing** batch data sources
# MAGIC - Write to **multiple** locations
# MAGIC
# MAGIC By default, **at-least-once** guarantee is provided. However, we can use batchId to handle it if the outputs operation is append only.
# MAGIC
# MAGIC You can also use the function *foreach* to apply arbitrary transformations on every row.
# MAGIC

# COMMAND ----------

# MAGIC %md Aditional Information when working with **writeStream**:
# MAGIC <br></br>
# MAGIC - **start** is used to start the execution of a streaming query.
# MAGIC - **awaitTermination** blocks the current thread and waits until the streaming query is terminated.

# COMMAND ----------

def process_batch(batch_df, batch_id):
    print(f"Batch ID: {batch_id}")
    print(f'Count View: {batch_df.filter(col("action") == "VIEW").count()}')
    print(f'Count Add to cart: {batch_df.filter(col("action") == "ADD_TO_CART").count()}')
    print(f'Count Purchase: {batch_df.filter(col("action") == "PURCHASE").count()}')
    print(f'Count Total: {batch_df.count()}\n')

query = df_transformed \
    .writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md #### Triggers

# COMMAND ----------

# MAGIC %md
# MAGIC Triggers define the timing of streaming processing.
# MAGIC <br></br>
# MAGIC - **Unspecified (default)**: Micro-batch runs as soon as possible.
# MAGIC - **Fixed interval**: Intervals are defined (e.g. 1 minute).
# MAGIC - **Once**: Only one micro-batch will be run, then the application stops.
# MAGIC - **AvailableNow**: Will process all the available data in multiple micro batches and then stop the application.
# MAGIC - **Continuous (experimental)**: Enables ~1ms end-to-end latency with at-least-once guarantee.
# MAGIC

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    print(f'Count Total: {batch_df.count()}\n')

query = df_transformed \
    .writeStream \
    .trigger(processingTime="15 seconds") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# Once
def process_batch(batch_df, batch_id):
    print(f'Count Total: {batch_df.count()}\n')

query = df_transformed \
    .writeStream \
    .trigger(once=True) \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# Available Now
def process_batch(batch_df, batch_id):
    print(f'Count Total: {batch_df.count()}\n')

query = df_transformed \
    .writeStream \
    .trigger(availableNow=True) \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md #### Output Modes
# MAGIC
# MAGIC It defines how the result data of a streaming query is written to a sink.<br></br>
# MAGIC - **Append (default)**: Appends only the new rows that arrive in the micro-batch window to the result table, suitable for streaming queries that generate new data without modifying existing rows.
# MAGIC - **Update**: Updates the result table with both new and modified rows, preserving the existing rows and reflecting changes over time.
# MAGIC - **Complete**: Outputs the entire result table after each micro-batch window, displaying all rows including new, modified, and existing ones.

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    print(f'Count Total: {batch_df.count()}\n')

query = df_transformed \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    batch_df.show()

query = df_transformed.groupBy("minute").count() \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    batch_df.show()

query = df_transformed.groupBy("minute").count() \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

# COMMAND ----------

# MAGIC %md # Checkpointing & Stateful Processing

# COMMAND ----------

# MAGIC %md #### Checkpointing

# COMMAND ----------

# MAGIC %md In the case of failures or when the job needs to stop processing for some time, it is possible to leverage checkpoint capabilities to **recover** the state of the application. Checkpoints ensure that the application will restart where it left off. To enable this, you just need to define a location for the checkpoint. All the progress will be stored, including offsets and read files.

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    print(f"Batch {batch_id} has {batch_df.count()} rows")

checkpoin_location = "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/checkpoint-test"

df_checkpoint = df_transformed \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoin_location) \
    .foreachBatch(process_batch) \
    .start()

df_checkpoint.awaitTermination()

# COMMAND ----------

# MAGIC %fs head s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/checkpoint-test/offsets/9

# COMMAND ----------

# MAGIC %md #### State Store

# COMMAND ----------

# MAGIC %md State store is a versioned key-value store which provides both **read** and **write** operations. In Structured Streaming, we use the state store provider to handle the stateful operations across batches. There are two built-in state store provider implementations.
# MAGIC <br></br>
# MAGIC - **HDFS (default)**: All the data is stored in memory map in the first stage, and then backed by files in an HDFS-compatible file system
# MAGIC - **RocksDB**: The best choice when dealing with millions of key-value pairs is to use RocksDB as the state store provider. Unlike the HDFS-backed implementation that stores data in the executor JVM memory, RocksDB efficiently utilizes native memory and local disk to manage the state. This optimization eliminates issues caused by memory pressure on the executor JVM and offers superior performance. All we need is to set the config `"spark.sql.streaming.stateStore.providerClass"`to `"com.databricks.sql.streaming.state.RocksDBStateStoreProvider"`
# MAGIC

# COMMAND ----------

# Fixed Interval
def process_batch(batch_df, batch_id):
    print(f"Batch {batch_id} has {batch_df.count()} rows")

checkpoin_location = "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/state-store-test"

df_state = df_transformed \
    .dropDuplicates(["action"]) \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoin_location) \
    .foreachBatch(process_batch) \
    .start()

df_state.awaitTermination()

# COMMAND ----------

# MAGIC %fs ls s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/state-store-test/

# COMMAND ----------

# MAGIC %md Disclaimer: There are many **stateful operations** in Spark (dropDuplicates, join, agg), and all of these are accessible only in the streaming transformations. In case you need stateful capabilities in the  **foreachBatch** you can create your own state manager.
# MAGIC

# COMMAND ----------

# MAGIC %md # Advanced Functions

# COMMAND ----------

# MAGIC %md #### Window Function & Aggregations

# COMMAND ----------

# MAGIC %md Aggregations over a event-time window are straightforward in Structured Streaming. You can perform any kind of **aggregation** operation (count, sum, etc.) for every window that corresponds to the event-time of a row.
# MAGIC
# MAGIC In our example, **10 minutes** represents the lenght of each window, and **5 minutes** means the slide of the window.

# COMMAND ----------

def process_batch(batch_df, batch_id):
    batch_df.orderBy(col("count").desc(), col("product_id").desc()).show(truncate=False)


checkpoin_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/window-test"
)

df_window = (
    df_transformed.groupBy(
        col("product_id"),
        col("action"),
        window(col("event_timestamp"), "10 minutes", "5 minutes"),
    )
    .count()
    .writeStream.trigger(processingTime="10 seconds")
    .outputMode("update")
    .option("checkpointLocation", checkpoin_location)
    .foreachBatch(process_batch)
    .start()
)

df_window.awaitTermination()

# COMMAND ----------

# MAGIC %md #### Watermark

# COMMAND ----------

# MAGIC %md Handling those late events is responsibility of the watermark.
# MAGIC
# MAGIC If an event is generated **06:01**, it could be received by the application at **06:11**. Since we are using event time, the job will update the window *06:00 - 06:10*, not the current window. Spark can mantain the state for a long period, but probably you would like to drop the old aggregations to save memory.
# MAGIC
# MAGIC The watermark will control it, we just need to define what is the **event time column** and the **threshold** of how late the events can be.

# COMMAND ----------

def process_batch(batch_df, batch_id):
    batch_df.orderBy(col("count").desc(), col("count").desc()).show(truncate=False)


checkpoin_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/watermark-test"
)

df_watermark = (
    df_transformed.withWatermark("event_timestamp", "5 minutes")
    .groupBy(
        col("product_id"),
        col("action"),
        window(col("event_timestamp"), "10 minutes"),
    )
    .count()
    .writeStream.trigger(processingTime="10 seconds")
    .outputMode("update")
    .option("checkpointLocation", checkpoin_location)
    .foreachBatch(process_batch)
    .start()
)

df_watermark.awaitTermination()

# COMMAND ----------

# MAGIC %md #### Join

# COMMAND ----------

# MAGIC %md This operation combines data from two or more Dataframes based on common columns when the values match. Structured streaming supports joining a streaming Dataframe with a static and another streaming Dataframe.
# MAGIC

# COMMAND ----------

# MAGIC %md **Static**: Not stateful. Types supported: Inner, Left Outer.

# COMMAND ----------

# Static Dataframe

static_df = spark.createDataFrame(
    [(i, f"Product {i}") for i in range(1, 101, 2)], # step=2
    ["product_id", "product_name"]
)

static_df.show(5)

# COMMAND ----------

def process_batch(batch_df, batch_id):
    batch_df.show(truncate=False)


checkpoin_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/join-static-test"
)

df_join_static = (
    df_transformed
    .join(
        broadcast(static_df), # Sort-merge Join is not necessary for such a small table
        on="product_id",
        how="left"
    )
    .writeStream.trigger(processingTime="10 seconds")
    .outputMode("update")
    .option("checkpointLocation", checkpoin_location)
    .foreachBatch(process_batch)
    .start()
)

df_join_static.awaitTermination()

# COMMAND ----------

# MAGIC %md # Sinks

# COMMAND ----------

# MAGIC %md A "sink" refers to the destination where the data is written after being processed by a Spark Structured Streaming query.
# MAGIC
# MAGIC Some common sink types in Spark Structured Streaming include:
# MAGIC
# MAGIC - **File Sink**: Writes the data to files in a file system like HDFS or a local file system.
# MAGIC - **Memory Sink**: Stores the data in memory as tables, which is useful for debugging and testing.
# MAGIC - **Console Sink**: Displays the data in the console for visual inspection and debugging.
# MAGIC - **Kafka Sink**: Writes the data to a Kafka topic.
# MAGIC - **JDBC Sink**: Writes the data to a JDBC-compliant database.
# MAGIC - **Custom Sink**: You can implement a custom sink to send the data to an external system or service.

# COMMAND ----------

# JDBC Example

def foreach_batch_function(batch_df, epoch_id):
    batch_df \
    .format("jdbc") \
    .option("url", "url") \
    .option("dbtable", "test") \
    .option("user", "postgres") \
    .option("password", "password") \
    .save()

df \
.writeStream \
.foreachBatch(foreach_batch_function) \
.start()

# COMMAND ----------

# MAGIC %md #### Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that brings **ACID** transactions to Apache Spark and big data workloads. It provides several enhancements to **data lakes**, making them more reliable, scalable, and performant. It combines **transaction logs** to control versions and the transactions, and also **parquet files** where the data is stored. Key Features:
# MAGIC <br></br>
# MAGIC - ACID Transactions
# MAGIC - Schema Evolution
# MAGIC - Time Travel
# MAGIC - Optimize, Z-Order & Vacuum
# MAGIC - Unified Batch and Streaming Processing

# COMMAND ----------

checkpoin_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/delta-test"
)

table_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/tables/delta-test"
)

df_delta = (
    df_transformed
    .writeStream.trigger(processingTime="10 seconds")
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoin_location)
    .start(table_location)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM DELTA.`s3://databricks-workspace-stack-0ca9a-bucket/tables/delta-test`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY DELTA.`s3://databricks-workspace-stack-0ca9a-bucket/tables/delta-test`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE DELTA.`s3://databricks-workspace-stack-0ca9a-bucket/tables/delta-test`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM DELTA.`s3://databricks-workspace-stack-0ca9a-bucket/tables/delta-test`
# MAGIC VERSION AS OF 2;

# COMMAND ----------


