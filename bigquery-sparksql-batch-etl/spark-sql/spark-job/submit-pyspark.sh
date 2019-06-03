gcloud dataproc jobs submit pyspark \
gs://sid-etl/spark-job/flights-etl.py --cluster=spark-dwh --region=us-east1 