from datetime import datetime,timedelta , date 

from airflow import models,DAG 

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataProcPySparkOperator,DataprocClusterDeleteOperator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators import BashOperator 

from airflow.models import *

from airflow.utils.trigger_rule import TriggerRule


current_date = str(date.today())

BUCKET = "gs://bucket_name"

PROJECT_ID = "your_project_id"

PYSPARK_JOB = BUCKET + "/spark-job/flights-etl.py"

DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'depends_on_past' : False,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "project_id":PROJECT_ID,
    "scheduled_interval":"30 2 * * *"
}

with DAG("flights_delay_etl",default_args=DEFAULT_DAG_ARGS) as dag : 

    create_cluster = DataprocClusterCreateOperator(

        task_id ="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-2",
        num_workers=2,
        region="asia-east1",
        zone ="asia-east1-a"
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id = "run_pyspark_etl",
        main = PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="asia-east1"
    )

    bq_load_delays_by_distance = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_avg_delays_by_distance",
        bucket=BUCKET,
        source_objects=["flights_data_output/"+current_date+"_distance_category/part-*"],
        destination_project_dataset_table=PROJECT_ID+".data_analysis.avg_delays_by_distance",
        autodetect = True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    bq_load_delays_by_flight_nums = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_delays_by_flight_nums",
        bucket=BUCKET,
        source_objects=["flights_data_output/"+current_date+"_flight_nums/part-*"],
        destination_project_dataset_table=PROJECT_ID+".data_analysis.avg_delays_by_flight_nums",
        autodetect = True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    delete_cluster = DataprocClusterDeleteOperator(

        task_id ="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="asia-east1",
        trigger_rule = TriggerRule.ALL_DONE
    )

    delete_tranformed_files = BashOperator(
        task_id = "delete_tranformed_files",
        bash_command = "gsutil -m rm -r " +BUCKET + "/flights_data_output/*"
    )

    create_cluster.dag = dag

    create_cluster.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream([bq_load_delays_by_flight_nums,bq_load_delays_by_distance,delete_cluster])

    delete_cluster.set_downstream(delete_tranformed_files)