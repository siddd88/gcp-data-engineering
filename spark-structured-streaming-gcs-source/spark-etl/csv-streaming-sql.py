from pyspark import SparkConf, SparkContext 
from pyspark.sql import SQLContext 
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import sys
import pprint
import urllib
import json

bucket_name = "gs://bucket_name" # Bucket where the streaming files are uploaded by the python script

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
    .option("checkpointLocation", bucket_name+"/streaming-files") \
    .schema(schema) \
    .csv(bucket_name+"/streaming-files")

csvDF.createOrReplaceTempView("browsing_data")


qry = """
	select 
		house_id,
		sum(overall_energy) as overall_energy,
		sum(dishwasher) as total_dishwasher,
		sum(furnace_1) as total_furnace1,
		sum(furnace_2) as total_furnace2,
		sum(fridge_consumption) as total_fridge_consumption
	from 
		browsing_data  
	group by 
		house_id
	"""

request_df = sqlcontext.sql(qry) 

query = request_df.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()