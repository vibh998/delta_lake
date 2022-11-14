-- Databricks notebook source
-- MAGIC %run /vs_unified_streaming-Test/unified_streaming

-- COMMAND ----------

-- MAGIC %python
-- MAGIC config_delta_table = 'default.streaming_config_details'
-- MAGIC dq_table = 'default.streaming_dq_config'
-- MAGIC exec_obj = Process('pid01', dq_table)
-- MAGIC query_pid01 , input_stream_df_pid01 = exec_obj.trigger_streams()
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC # config_delta_table = 'default.streaming_config_details'
-- MAGIC # dq_table = 'default.streaming_dq_config'
-- MAGIC exec_obj = Process('pid02', dq_table)
-- MAGIC query_pid02 , input_stream_df_pid02  = exec_obj.trigger_streams()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC 
-- MAGIC ##implement stream joins with 
-- MAGIC # start stream 1
-- MAGIC # start stream 2
-- MAGIC # merge streaming dataframes and write results to gold table
-- MAGIC query_pid01.start() 
-- MAGIC query_pid02.start()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # import pyspark.sql.types as T 
-- MAGIC # import pyspark.sql.functions as F
-- MAGIC # from pyspark.sql.functions import expr
-- MAGIC 
-- MAGIC # ip1_withwatermark = input_stream_df_pid01.withWatermark("shipping_limit_date" , "10 minutes")
-- MAGIC # ip2_withwatermark = input_stream_df_pid02.withWatermark("order_purchase_timestamp" , "10 minutes")
-- MAGIC 
-- MAGIC # dup_cols = ["_execute_timestamp", "_filename", "_record_type" , "_unique_id", "order_id"  ]
-- MAGIC 
-- MAGIC # display (ip1_withwatermark.join(ip2_withwatermark , (ip1_withwatermark.order_id == ip2_withwatermark.order_id) &\
-- MAGIC #                       (ip2_withwatermark.order_purchase_timestamp <= ip1_withwatermark.shipping_limit_date + expr("INTERVAL 2 MINUTES")),"leftOuter"))
-- MAGIC 
-- MAGIC 
-- MAGIC #.drop(*dup_cols).writeStream.option( "checkpointLocation", "/tmp/delta/_checkpoints/gold_merge").toTable("joined_order_item_gold")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from delta.tables import *
-- MAGIC 
-- MAGIC gold_join_delta = "order_items_join_gold"
-- MAGIC spark.sql(f""" CREATE TABLE IF NOT EXISTS {gold_join_delta } (order_id_items string,
-- MAGIC order_item_id int,
-- MAGIC product_id string, 
-- MAGIC seller_id string, 
-- MAGIC shipping_limit_date timestamp, 
-- MAGIC price double,
-- MAGIC freight_value float ,
-- MAGIC customer_id string, 
-- MAGIC order_status string,
-- MAGIC order_purchase_timestamp timestamp, 
-- MAGIC _filename_orders string ,
-- MAGIC _filename_items string
-- MAGIC )
-- MAGIC """)
-- MAGIC #get delta table
-- MAGIC 
-- MAGIC deltaTableGold = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/'+str(gold_join_delta))
-- MAGIC # def merge_data(batchDf, batchId):
-- MAGIC #   display(batchDf)
-- MAGIC #   deltaTableGold.alias('gold').merge(\
-- MAGIC #                                      batchDf.alias("updates"), 'gold.order_id_item == updates.order_id_item'
-- MAGIC #                                     ).whenMatchedUpdateAll().whenNotMatchedInsertAll()
-- MAGIC   
-- MAGIC   
-- MAGIC   
-- MAGIC   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark_read_options = {
-- MAGIC   "maxFilesPerTrigger" : 1,
-- MAGIC   "mergeSchema": True,
-- MAGIC }
-- MAGIC 
-- MAGIC spark_write_options = {  
-- MAGIC   "OutputMode" : "append",
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.types as T 
-- MAGIC import pyspark.sql.functions as F
-- MAGIC 
-- MAGIC ##join  delta table name 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ##read stream1
-- MAGIC silver1_wtm  =  (
-- MAGIC   spark
-- MAGIC   .readStream
-- MAGIC   .format("delta")
-- MAGIC   .option("maxFilesPerTrigger", "1")
-- MAGIC   .option("ignoreChanges", "true")
-- MAGIC   .option("withEventTimeOrder", "true")
-- MAGIC   .table("order_items_silver")
-- MAGIC   .select(F.col("order_id").alias("order_id_items") , F.col("order_item_id")  ,\
-- MAGIC    F.col("product_id")   ,F.col("seller_id")  , F.col("shipping_limit_date").cast(T.TimestampType()).alias("shipping_limit_date"), F.col("price") , F.col("_filename").alias("_filename_items"))
-- MAGIC )
-- MAGIC 
-- MAGIC ##create watermark
-- MAGIC silver1_wtm = silver1_wtm.withWatermark("shipping_limit_date" , "2 minutes")
-- MAGIC 
-- MAGIC ##read stream2
-- MAGIC silver2_wtm  =  (
-- MAGIC   spark
-- MAGIC   .readStream
-- MAGIC   .format("delta")
-- MAGIC   .option("maxFilesPerTrigger", "1")
-- MAGIC   .option("ignoreChanges", "true")
-- MAGIC   .option("withEventTimeOrder", "true")
-- MAGIC   .table("order_dataset_silver")
-- MAGIC   .select(F.col("order_id").alias("order_id_order") , F.col("customer_id")  ,\
-- MAGIC    F.col("order_status")   , F.expr("to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' )").alias("order_purchase_timestamp") , F.col("_filename").alias("_filename_orders"))
-- MAGIC )
-- MAGIC ##create watermark
-- MAGIC silver2_wtm = silver2_wtm.withWatermark( "order_purchase_timestamp", "2 minutes")
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC df_gold_final = (
-- MAGIC   silver1_wtm
-- MAGIC   .join(silver2_wtm , (silver1_wtm.order_id_items == silver2_wtm.order_id_order)\
-- MAGIC   & (silver1_wtm.shipping_limit_date >= silver2_wtm.order_purchase_timestamp)\
-- MAGIC   & (silver1_wtm.shipping_limit_date <= silver2_wtm.order_purchase_timestamp + expr("INTERVAL 1 MINUTES")),"inner")
-- MAGIC   .select(  F.col("order_id_items") ,  F.col("order_item_id")  ,\
-- MAGIC    F.col("product_id")   ,F.col("seller_id")  , F.col("shipping_limit_date"), F.col("price") , F.col("_filename_items"),F.col("order_id_order") , F.col("customer_id")  ,\
-- MAGIC    F.col("order_status")   ,F.col("order_purchase_timestamp")  , F.col("_filename_orders"))
-- MAGIC   .dropDuplicates(["order_id_items"])
-- MAGIC )
-- MAGIC 
-- MAGIC 
-- MAGIC #display(df_gold_final)
-- MAGIC 
-- MAGIC 
-- MAGIC #display (df_gold_final.writeStream.queryName("join_streams").option( "checkpointLocation", "/tmp/delta/_checkpoints/gold_merge").start())
-- MAGIC   
-- MAGIC   
-- MAGIC   
-- MAGIC   ##select needed fields from joined df
-- MAGIC 
-- MAGIC # query = (
-- MAGIC #   df_gold_final
-- MAGIC #   .writeStream
-- MAGIC #   .foreachBatch(merge_data)
-- MAGIC #   .queryName("join_streams").option( "checkpointLocation", "/tmp/delta/_checkpoints/gold_merge").toTable(gold_join_delta)
-- MAGIC # )
-- MAGIC 
-- MAGIC df_2 = df_gold_final.writeStream.queryName("join_streams").format('delta').outputMode("append")\
-- MAGIC .option("checkpointLocation", "/tmp/delta/_checkpoints/gold_merge").toTable(gold_join_delta)
-- MAGIC 
-- MAGIC 
-- MAGIC #dropDuplicates("ExceptionId", "LastUpdateTime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # def merge_data(batchDf, batchId):
-- MAGIC #   batchDf = batchDf.alias('updates')
-- MAGIC #   deltaTableGold.alias('gold') \
-- MAGIC #   .merge(
-- MAGIC #     batchDf.alias('updates'), 'gold.order_id_item = updates.order_id_item'
-- MAGIC #   ).whenMatchedUpdate(set = {  "order_id_items": updates.order_id_items,
-- MAGIC #       "order_item_id" : updates.order_item_id ,
-- MAGIC #   "product_id" : updates.product_id, 
-- MAGIC #       "seller_id" : updates.seller_id,
-- MAGIC #       "shipping_limit_date" : updates.shipping_limit_date,
-- MAGIC #       "price" : updates.price,
-- MAGIC #       "freight_value" : updates.freight_value ,
-- MAGIC #       "customer_id" : updates.customer_id,
-- MAGIC #       "order_status" : updates.order_status ,
-- MAGIC #   "order_purchase_timestamp" : updates.order_purchase_timestamp,
-- MAGIC #       "_filename_orders" : updates._filename_orders,
-- MAGIC #       "_filename_items" : updates._filename_items
-- MAGIC #     }\
-- MAGIC #   ) \
-- MAGIC #   .whenNotMatchedInsert(values =\
-- MAGIC #     {
-- MAGIC #       "order_id_items": updates.order_id_items,
-- MAGIC #       "order_item_id" : updates.order_item_id ,
-- MAGIC #   "product_id" : updates.product_id, 
-- MAGIC #       "seller_id" : updates.seller_id,
-- MAGIC #       "shipping_limit_date" : updates.shipping_limit_date,
-- MAGIC #       "price" : updates.price,
-- MAGIC #       "freight_value" : updates.freight_value ,
-- MAGIC #       "customer_id" : updates.customer_id,
-- MAGIC #       "order_status" :updates.order_status ,
-- MAGIC #   "order_purchase_timestamp" :updates.order_purchase_timestamp,
-- MAGIC #       "_filename_orders" : updates._filename_orders,
-- MAGIC #       "_filename_items" : updates._filename_items  }
-- MAGIC #   ) \
-- MAGIC #   .execute()
