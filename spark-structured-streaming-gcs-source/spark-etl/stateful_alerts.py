from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json 

from kafka import KafkaProducer

bucket_name = "gs://bucket_name" # Bucket where the streaming files are uploaded by the python script

cluster_name = "spark-sql-etl-cluster"

producer = KafkaProducer(bootstrap_servers=[cluster_name+'-w-0:9092',cluster_name+'-w-1:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))

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
    .option("checkpointLocation",bucket_name+"/checkpoint") \
    .schema(schema) \
    .csv(bucket_name+"/streaming_files")

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
                    sum(col('living_room_consumption')).alias('total_living_room_energy')
                    )


def foreach_batch_function(df,epoch_id) : 

    overall_energy_threshold = "10"

    df_alert = df.select("house_id","overall_energy").where("overall_energy>="+overall_energy_threshold)

    alerts = df_alert.toJSON()

    if not alerts.isEmpty() :

        for row in alerts.collect() : 

            producer.send('high_consumption_alerts',value=row)


query = (
        df_aggregated.writeStream \
        .foreachBatch(foreach_batch_function)
        .outputMode("update")
        .start()
    )

query.awaitTermination()