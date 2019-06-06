from pyspark.sql import HiveContext 
from pyspark import SparkContext 

import time 

sc = SparkContext()

hc = HiveContext(sc)

folder_name = int(round(time.time() * 1000))

qry = """
		select 
			date(date_time) as event_date,
			type,
			state,
			category,
			count(*) as visit_count 
		from 
			user_server_logs 
		where 
			pid is null 

		group by 
			1,2,3,4
		"""

visit_count = hc.sql(qry)

visit_count.coalesce(1).write.format('parquet').save("gs://sidd-streaming/streaming-etl-output/"+str(folder_name))
