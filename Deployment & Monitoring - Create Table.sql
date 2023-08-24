-- Databricks notebook source
DROP TABLE IF EXISTS clickstream;
CREATE TABLE clickstream
(
  user_id integer,
  product_id integer,
  product_name string,
  action string,
  window_start timestamp,
  window_end timestamp,
  count long
)
USING DELTA
PARTITIONED BY (`action`)
LOCATION 's3://databricks-workspace-stack-0ca9a-bucket/tables/clickstream'

-- COMMAND ----------

-- MAGIC %fs rm -r s3://databricks-workspace-stack-0ca9a-bucket/tables

-- COMMAND ----------

-- MAGIC %fs rm -r s3://databricks-workspace-stack-0ca9a-bucket/checkpoints

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from clickstream where user_id = 18 and product_id = 44 and action = "view" order by count desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from clickstream where user_id != 1 order by count desc

-- COMMAND ----------


