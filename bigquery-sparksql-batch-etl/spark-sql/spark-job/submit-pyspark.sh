gcloud dataproc jobs submit pyspark \
gs://your_bucket_name/spark-job/flights-etl.py --cluster=spark-dwh --region=us-east1 