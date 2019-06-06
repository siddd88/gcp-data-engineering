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

spark = SparkSession \
    .builder \
    .appName("user-logs-analysis-streaming") \
    .getOrCreate()

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "spark-etl-w-1:9092") \
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
        StructField('sub_cat', StringType(), True)
   ]
)

df_parsed = df.select("value")

df_visits = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))

df_tumbling_window = df_visits \
    .where("pid is not null") \
    .withWatermark("date_time", "20 minutes") \
    .groupBy(
        df_visits.sub_cat,
        df_visits.type) \
    .count()


# df_tumbling_window = df_visits \
#     .where("pid is not null") \
#     .withWatermark("date_time", "2 minutes") \
#     .groupBy(
#         window(df_visits.date_time, "300 seconds"),
#         df_visits.sub_cat,
#         df_visits.type) \
#     .count()


# query = df_windowed_counts.writeStream \
#             .outputMode("complete") \
#             .format("console") \
#             .start()

def foreach_batch_function(df,epoch_id) :
    print(epoch_id)
    print("--------------------")
    df.show()
    
    df.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation","gs://sidd-streams/spark-agg-checkpoints/") \
    .option("path","gs://sidd-streams/stateful_aggregations/") \
    .save()
        
query = df_tumbling_window.writeStream.trigger(processingTime="20 seconds") \
          .outputMode("complete") \
          .format("console") \
          .start()

# query = (
#     df_tumbling_window.writeStream.trigger(processingTime="10 seconds") \
#     .foreachBatch(foreach_batch_function)
#     .outputMode("complete")
#     .start()
#     )


query.awaitTermination()