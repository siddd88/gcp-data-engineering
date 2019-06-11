from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import *

bucket_streaming_files = "gs://bucket_name" # Bucket to read the streaming files from 

bucket_transformed_output = "gs://bucket_name" # Bucket to write the transformed output to 

conf = SparkConf(). \
setAppName("Streaming Data"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)

schema = StructType(
    [
        StructField('date_time', TimestampType(), True),
        StructField('house_id', IntegerType(), True),
        StructField('overall_energy', DoubleType(), True),
        StructField('dishwasher', DoubleType(), True),
        StructField('furnace_1', DoubleType(), True),
        StructField('furnace_2', DoubleType(), True),
        StructField('fridge_consumption', DoubleType(), True),
        StructField('wine_cellar_consumption', DoubleType(), True),
        StructField('microwave_consumption', DoubleType(), True),
        StructField('living_room_consumption', DoubleType(), True)
   ]
)

csvDF = sqlcontext \
    .readStream \
    .option("sep", ",") \
    .option("checkpointLocation",bucket_streaming_files+"/checkpoint") \
    .schema(schema) \
    .csv(bucket_streaming_files+"/streaming_files")

df_aggregated = csvDF \
                .withWatermark("date_time","1 day") \
                .groupBy(
                    csvDF.house_id) \
                .agg(
                    sum(col('overall_energy')).alias('overall_energy'),
                    sum(col('dishwasher')).alias('total_dishwasher_energy'),
                    sum(col('furnace_1')).alias('total_furnace1_energy'),
                    sum(col('furnace_2')).alias('total_furnace2_energy'),
                    sum(col('fridge_consumption')).alias('total_fridge_energy'),
                    sum(col('wine_cellar_consumption')).alias('total_wine_cellar_energy'),
                    sum(col('microwave_consumption')).alias('total_microwave_energy'),
                    sum(col('living_room_consumption')).alias('total_living_room_energy'),
                    max(col('date_time')).alias("latest_event_datetime")
                    )

def foreach_batch_function(df,epoch_id) :
    
    df_final  = df.dropDuplicates(subset=["house_id"])
    
    df_final.coalesce(1).write \
    .format("avro") \
    .mode("overwrite") \
    .option("checkpointLocation",bucket_transformed_output+"/iot-data-checkpoints/") \
    .option("path",bucket_transformed_output+"/iot_transformed_data/") \
    .save()

query = (
        df_aggregated.writeStream.trigger(processingTime="60 seconds") \
        .foreachBatch(foreach_batch_function)
        .outputMode("complete")
        .start()
    )

query.awaitTermination()