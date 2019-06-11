from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time 
import datetime
conf = SparkConf(). \
setAppName("Streaming Data"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)

cluster_name = "gs://cluster_name" # cluster in which kafka & zookeeper are installed  

bucket_name = "gs://your_bucket_name"

spark = SparkSession \
    .builder \
    .appName("user-logs-analysis-streaming") \
    .getOrCreate()

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", cluster_name+"-w-1:9092") \
      .option("subscribe", "user_browsing_logs") \
      .option("failOnDataLoss","false") \
      .load() \
      .selectExpr("CAST(value AS STRING) ")

schema = StructType(
    [
        StructField('category', StringType(), True),
        StructField('date_time', TimestampType(), True),
        StructField('type', StringType(), True),
        StructField('pid', IntegerType(), True),
        StructField('state', StringType(), True),
        StructField('sub_cat', StringType(), True),
        StructField('ip_address', StringType(), True)
   ]
)

df_parsed = df.select("value")

df_streaming_visits = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))



df_tumbling_window = df_streaming_visits \
                .where("pid is null") \
                .withWatermark("date_time","10 minutes") \
                .groupBy(       
                    window(df_streaming_visits.date_time,"240 seconds"),           
                    df_streaming_visits.type,
                    df_streaming_visits.category,
                    df_streaming_visits.state) \
                .count()

def foreach_batch_function(df,epoch_id) :

    df_final = df.selectExpr("window.start as window_start_time","window.end as window_end_time",
                        "type","category","state","count")

    df_final.show(10,False)

    df_final.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation",bucket_name+"/spark_checkpoints/") \
    .option("path",bucket_name+"/visits_by_categories/") \
    .save()

query = (

        df_tumbling_window.writeStream.trigger(processingTime="100 seconds") \
        .foreachBatch(foreach_batch_function)
        .outputMode("complete")
        .start()
    )


query.awaitTermination()
