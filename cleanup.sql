-- Databricks notebook source
-- MAGIC %sql
-- MAGIC 
-- MAGIC delete from order_dataset_bronze;
-- MAGIC delete from order_dataset_silver;
-- MAGIC 
-- MAGIC drop table order_dataset_bronze;
-- MAGIC drop table order_dataset_silver;
-- MAGIC delete from order_dataset_gold;
-- MAGIC drop table order_dataset_gold;
-- MAGIC 
-- MAGIC delete from order_items_bronze;
-- MAGIC delete from order_items_silver;
-- MAGIC 
-- MAGIC drop table order_items_bronze;
-- MAGIC drop table order_items_silver;
-- MAGIC delete from order_items_gold;
-- MAGIC drop table order_items_gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/bronze", recurse = True)
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/silver", recurse = True)
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/gold", recurse = True)
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/gold_merge", recurse = True)
-- MAGIC 
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/", recurse = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_dataset_bronze', recurse = True)
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_dataset_silver', recurse = True)
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_dataset_gold', recurse = True)
-- MAGIC 
-- MAGIC 
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_items_bronze', recurse = True)
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_items_silver', recurse = True)
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_items_gold', recurse = True)
-- MAGIC 
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/joined_order_item_gold', recurse = True)
-- MAGIC 
-- MAGIC 
-- MAGIC #dbutils.fs.rm('dbfs:/user/hive/warehouse/streaming_log', recurse = True)

-- COMMAND ----------

delete from streaming_log;
delete from order_items_join_gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_items_join_gold', recurse = True)
-- MAGIC 
-- MAGIC #dbutils.fs.rm ('dbfs:/FileStore/tables/', recurse = True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.rm("/tmp/delta/_checkpoints/gold_merge", recurse = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/order_items_join_gold', recurse = True)

-- COMMAND ----------


