gcloud dataproc jobs submit pyspark \
spark-etl/stateful_alerts.py \
--cluster=spark-sql-etl-cluster \
--region=asia-northeast1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,jar-files/spark-sql-kafka-0-10_2.11-2.4.2.jar 


gcloud dataproc jobs submit pyspark \
spark-etl/csv-dataframe-aggregations.py \
--cluster=spark-sql-etl-cluster \
--region=asia-northeast1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.11-2.4.2.jar,jar-files/spark-sql-kafka-0-10_2.11-2.4.2.jar \
--properties spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.2
