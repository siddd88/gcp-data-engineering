from datetime import datetime, timedelta
from airflow import models
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import BashOperator

from airflow.utils.trigger_rule import TriggerRule

BUCKET = "gs://bucket_name"
PROJECT_ID = "project_id"
instance_name = "your_cloudsql_instance_name"

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'project_id': PROJECT_ID, 
    'schedule_interval': "30 2 * * *"
}


with DAG('sqoop-incremental-dag',
         default_args=DEFAULT_DAG_ARGS) as dag:
   
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_sqoop_cluster',
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-2',
        init_actions_uris=["gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh"],
        num_workers=2,
        region='asia-east1',
        zone='asia-east1-a',
        service_account_scopes=["https://www.googleapis.com/auth/sqlservice.admin"],
        properties={"hive:hive.metastore.warehouse.dir":BUCKET+"/hive-warehouse"},
        metadata={"additional-cloud-sql-instances":instance_name+"=tcp:3307","enable-cloud-sql-hive-metastore":"false"},
        image_version="1.2"
    )

    sqoop_inc_import = BashOperator(
        task_id= 'sqoop_incremental_import',
        bash_command="bash /home/airflow/gcs/plugins/sqoop-incremental-imports.sh ephemeral-spark-cluster-{{ds_nodash}}",
        dag=dag
    )

    hdfs_to_gcs = BashOperator(

        task_id="hdfs_to_gcs",
        bash_command="gcloud compute ssh ephemeral-spark-cluster-{{ds_nodash}}-m --zone='asia-east1-a' -- -T 'hadoop distcp /incremental_appends/*.avro gs://bucket_name/sqoop_output/'",
        dag=dag       
    )

    bq_load_flight_delays = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_flight_delays",
        bucket="sidd-etl",
        source_objects=["sqoop_output/part.20190515_*.avro"],
        destination_project_dataset_table=PROJECT_ID+".data_analysis.flights_delays",
        autodetect=True,
        source_format="AVRO",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0   
    )

    # delete_cluster = DataprocClusterDeleteOperator(
    #     task_id='delete_dataproc_cluster',
    #     cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
    #     region='asia-east1',
    #     trigger_rule=TriggerRule.ALL_DONE
    # )

     


    create_cluster.dag = dag

    create_cluster.set_downstream(sqoop_inc_import)
    sqoop_inc_import.set_downstream(hdfs_to_gcs)
    hdfs_to_gcs.set_downstream(bq_load_flight_delays)
#    bq_load_delays_by_distance.set_downstream(delete_cluster)

