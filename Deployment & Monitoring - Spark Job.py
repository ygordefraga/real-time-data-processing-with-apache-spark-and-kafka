# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

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
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "kafka.group.id": "group-11",
}

df_read = (
    spark
    .readStream
    .format("kafka")
    .options(**kafka_consumer_properties)
    .load()
)

# COMMAND ----------

json_schema = "user_id INT, product_id INT, action STRING, event_timestamp STRING"


# Static Dataframe

static_df = spark.createDataFrame(
    [(i, f"Product {i}") for i in range(1, 101)],
    ["product_id", "product_name"]
)

df_transformed = (
    df_read
    .select(from_json(col("value").cast("string"), json_schema).alias("json_data"))
    .select("json_data.*")
    .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["user_id", "product_id", "action", "event_timestamp"])
    .groupBy(
        col("user_id"),
        col("product_id"),
        col("action"),
        window(col("event_timestamp"), "10 minutes"),
    )
    .count()
    .join(
        broadcast(static_df),
        on="product_id",
        how="left"
    )
    .repartition(8, "action")
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
)

# COMMAND ----------

def process_batch(batch_df, batch_id):
    clickstream = (
        DeltaTable
        .forPath(spark, 's3://databricks-workspace-stack-0ca9a-bucket/tables/clickstream')
    )

    batch_df.show()

    clickstream.alias('c').merge(
        batch_df.alias('b'),
        """
        c.user_id = b.user_id
        AND c.product_id = b.product_id 
        AND c.action = b.action
        AND c.window_start = b.window_start   
        """
    ).whenMatchedUpdate(set = {"c.count": "b.count"}).whenNotMatchedInsertAll().execute()

checkpoin_location = (
    "s3://databricks-workspace-stack-0ca9a-bucket/checkpoints/final-application"
)

query = (
    df_transformed
    .writeStream.trigger(processingTime="20 seconds")
    .outputMode("update")
    .option("checkpointLocation", checkpoin_location)
    .foreachBatch(process_batch)
    .start()
)

# COMMAND ----------


