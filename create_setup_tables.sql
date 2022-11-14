-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/bronze", recurse = True)
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/silver", recurse = True)
-- MAGIC dbutils.fs.rm("dbfs:/tmp/delta/_checkpoints/gold", recurse = True)
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/', recurse = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm ('dbfs:/user/hive/warehouse/streaming_log', recurse = True)

-- COMMAND ----------

--create file setup
CREATE TABLE if not exists streaming_config_details (
process_id string not null ,
source_bucket_path string not null ,
source_file_format string not null ,
source_file_pattern string not null ,
sink_delta_table string not null ,
column_delimeter string ,
preprocessing_steps string  ,
min_number_of_records bigint ,
max_latency_hours string ,
active string ,
checkpoint string,
groupbykey string,
count_col string
)


-- COMMAND ----------

--add config rows

insert into streaming_config_details values (
'pid01',
'dbfs:/FileStore/tables/',
'csv',
'olist_order_items_*.csv',
'default.order_items_bronze',
',',
'{1:"readFilesAscLocal", 2 : "PGPdecryptFile"}' , --other options
100,
'undefined',
'y',
'/tmp/delta/_checkpoints/',
'shipping_limit_date',
'order_id'

);


insert into streaming_config_details values (
'pid02',
'dbfs:/FileStore/tables/',
'csv',
'olist_orders_dataset_*.csv',
'default.order_dataset_bronze',
',',
'{1:"readFilesAscLocal", 2 : "PGPdecryptFile"}' , --other options
100,
'undefined',
'y',
'/tmp/delta/_checkpoints/',
'order_purchase_timestamp',
'order_id'
);

-- COMMAND ----------

 --create log setup
-- create TABLE if not exists streaming_log_details (
-- process_id string not null ,
-- batchID string not null ,
-- file_name string not null ,
-- file_table_size string,
-- file_table_import_status string,
-- file_table_records bigint,
-- file_error_records bigint,
-- ingestion_timestamp timestamp,
-- load_comment string,
-- current_dttm timestamp
-- )

-- COMMAND ----------

--create dq rules setup

-- create TABLE if not exists streaming_dq_rules (
-- process_id string not null ,
-- batchID string not null ,
-- file_name string not null ,
-- file_table_size string
-- file_table_import_status string
-- file_table_records bigint
-- file_error_records bigint
-- ingestion_timestamp timestamp
-- load_comment string
-- current_dttm timestamp
-- )

-- COMMAND ----------

--create stream_dq_config

CREATE TABLE if not exists streaming_dq_config (
process_id string,
col_name string,
is_primary_key string,
is_required string,
convert_hash string,
formatting_rules string,
active string
)

-- COMMAND ----------

insert into streaming_dq_config values (
'pid02',
'order_id',
'y',
'y',
'y',
"{1:'UPPER'}",
'y'


);



insert into streaming_dq_config values (
'pid02',
'order_purchase_timestamp',
'n',
'y',
'y',
"{1:'UPPER' , 2:'WITHOUT_SPECIAL_CHARS' ,  3: 'CONVERT_DTTM_FORMAT' }",
'y'
);


insert into streaming_dq_config values (
'pid01',
'order_id',
'y',
'y',
'y',
"{1:'UPPER'}",
'y'


);


insert into streaming_dq_config values (
'pid01',
'shipping_limit_date',
'n',
'y',
'y',
"{1:'UPPER' , 2:'WITHOUT_SPECIAL_CHARS' ,  3: 'CONVERT_DTTM_FORMAT' }",
'y'


);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC --create silver table schema
-- MAGIC 
-- MAGIC -- create table default.order_dataset_silver (
-- MAGIC -- order_id string,
-- MAGIC -- customer_id string,
-- MAGIC -- order_status string,
-- MAGIC -- order_purchase_timestamp timestamp,
-- MAGIC -- order_approved_at timestamp,
-- MAGIC -- order_delivered_carrier_date timestamp,
-- MAGIC -- order_delivered_customer_date timestamp,
-- MAGIC -- order_estimated_delivery_date timestamp,
-- MAGIC -- _filename string,
-- MAGIC -- _unique_id string,
-- MAGIC -- _execute_timestamp timestamp,
-- MAGIC -- _record_type string
-- MAGIC -- )

-- COMMAND ----------

 CREATE TABLE if not exists streaming_log (
processid string,
batchID int,
filename string,
filesize string,
file_table_import_status string,
formatting_rules string,
filecount int,
file_error_records int,
ingestion_timestamp timestamp,
comment string,
current_timestamp timestamp


)

-- COMMAND ----------

--select * from default.order_dataset_silver where _filename ='dbfs:/FileStore/tables/olist_orders_dataset_22102016.csv'


select shipping_limit_date, cast ( to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' ) AS TIMESTAMP)from order_items_silver A left outer join order_dataset_silver B on A.order_id = B.order_id
where A.shipping_limit_date >= cast ( to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' ) AS TIMESTAMP)
and A.shipping_limit_date <= cast ( to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' ) AS TIMESTAMP) + INTERVAL 1 MINUTES
;
--select * from default.order_dataset_bronze where 

-- COMMAND ----------

select order_purchase_timestamp , to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' )  , cast ( to_timestamp (order_purchase_timestamp ,'dd-MM-yyyy HH:mm:SS' ) AS TIMESTAMP) from default.order_dataset_silver ;
