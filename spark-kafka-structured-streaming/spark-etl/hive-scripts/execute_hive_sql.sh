

gcloud dataproc jobs submit hive \
    --cluster hive-sql --region us-central1 \
    --execute "
      CREATE EXTERNAL TABLE user_server_logs
      (category STRING,date_time timestamp,type STRING,pid INTEGER,state STRING,sub_cat STRING, ip_address STRING)
      STORED AS PARQUET
      LOCATION 'gs://sidd-streaming/streaming_data_output';"

gcloud dataproc jobs submit hive \
    --cluster hive-sql --region us-central1 \
    --execute "select * from user_server_logs limit 10"

gcloud dataproc jobs submit hive \
    --cluster hive-sql --region us-central1 \
    --execute "drop table user_server_logs"


gcloud dataproc jobs submit hive \
    --cluster hive-sql --region us-central1 \
    --execute "    select 
        date(date_time) as event_time,
        sub_cat,
        substr(date_time, 12,5) as time_added,
        collect_set(pid) as products,
        count(*) as product_count
    from user_server_logs
    where 
        pid is not null
    group by date(date_time),sub_cat,substr(date_time, 12,5)"


    gcloud dataproc jobs submit hive \
    --cluster hive-sql --region us-central1 \
    --execute "    select 
        date_time,
        type,
        state,
        collect_set(pid) as products_added
    from user_server_logs
    where pid is not null
    group by 
    date_time,type,state,substr(date_time, 12,5)
    limit 10
    "

gcloud dataproc jobs submit hive \
    --cluster pyspark-dataproc \
    --execute "
      SELECT *
      FROM browsing_logs
      LIMIT 40;"

gcloud dataproc jobs submit hive \
    --cluster pyspark-dataproc \
    --execute "
      SELECT
        date_time,
        substr(date_time, 12,8) time,
        state,
        category,
        count(*) as products_added 
        FROM browsing_logs
        group by 
          date_time,
          substr(date_time, 12,8),
          state,
          category
        limit 10
        ;"
  