from datetime import datetime, timedelta
from airflow import models
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

BUCKET = "gs://bucket_name"
instance_name = "your_cloudsql_instance_name"
project_id = "your_project_id"

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'project_id': project_id, 
    'schedule_interval': "30 2 * * *"
}


# Create Directed Acyclic Graph for Airflow
with DAG('create_dp_cluster',
         default_args=DEFAULT_DAG_ARGS) as dag:  # Here we are using dag as context.
   
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
        metadata={"additional-cloud-sql-instances":instance_name,"enable-cloud-sql-hive-metastore":"false"},
        image_version="1.2"
    )

    
    create_cluster.dag = dag
