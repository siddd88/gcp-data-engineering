from airflow import models,DAG 
from datetime import datetime,timedelta

from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

from airflow.models import * 

BUCKET = "gs://your_bucket_name"

PYSPARK_JOB = BUCKET + "/sparksql-hive/hivesql-etl.py"

DEFAULT_DAG_ARGS = {
	"owner":"airflow",
	"depends_on_past":True,
	"start_date":datetime.utcnow(),
	"email_on_failure":False,
	"retries":1,
	"retry_delay":timedelta(minutes=5),
	"project_id":"your_project_id",
	"schedule_interval":"*/15 * * * * "
}

with DAG("visits_by_category",default_args=DEFAULT_DAG_ARGS) as dag : 

	submit_pyspark = DataProcPySparkOperator(
		task_id="run_sparksql_etl",
		main=PYSPARK_JOB,
		cluster_name="hive-sql",
		region="us-central1"
	)

	submit_pyspark.dag = dag