from datetime import datetime, timedelta
from airflow import models
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator,DataProcHadoopOperator, DataprocClusterDeleteOperator

from airflow.contrib.hooks.sqoop_hook import SqoopHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import BashOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# BUCKET = models.Variable.get('gcs_bucket')
# OUTPUT_TABLE = models.Variable.get('bq_output_table')


BUCKET = "gs://sidd-etl"
OUTPUT_TABLE1 = "avg_delays_by_flight_nums"

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 'bigdata-etl-240212', 
    'schedule_interval': "30 2 * * *"
}


# Create Directed Acyclic Graph for Airflow
with DAG('sqoop-data-etl',
         default_args=DEFAULT_DAG_ARGS) as dag:  # Here we are using dag as context.
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
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
        properties={"hive:hive.metastore.warehouse.dir":"gs://sidd-etl/hive-warehouse"},
        metadata={"additional-cloud-sql-instances":"bigdata-etl-240212:us-central1:mysql-instance=tcp:3307","enable-cloud-sql-hive-metastore":"false"},
        image_version="1.2"
    )

    # Submit the sqoop job.
    sqoop_import = BashOperator(
        task_id= 'sqoop_import',
        bash_command="bash /home/airflow/gcs/plugins/sqoop-inc-imports.sh ephemeral-spark-cluster-{{ds_nodash}}",
        dag=dag
    )


    move_output_files = BashOperator(
        task_id = "move_files",
        bash_command="gcloud compute ssh ephemeral-spark-cluster-{{ds_nodash}}-m --zone='asia-east1-a' -- -T 'hadoop distcp /sqoop_output/*.avro gs://sidd-etl/sqoop_output/'",
        dag=dag
    )



    create_cluster.dag = dag

    create_cluster.set_downstream(sqoop_import)
    sqoop_import.set_downstream(move_output_files)

    # submit_sqoop.set_downstream(delete_cluster)    

