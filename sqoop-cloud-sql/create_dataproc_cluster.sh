bucket="gs://your_bucket_name"
cluster_name="ephemeral-spark-cluster-20190518"
instance_name="your_cloudsql_instance_name"

gcloud dataproc clusters create $cluster_name \
--zone=us-central1-a \
--scopes=default,sql-admin \
--initialization-actions=gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
--properties=hive:hive.metastore.warehouse.dir=$bucket/hive-warehouse \
--metadata=enable-cloud-sql-hive-metastore=false \
 --metadata=additional-cloud-sql-instances=$instance_name=tcp:3307 \
 --master-machine-type n1-standard-1 \
 --master-boot-disk-size 20 \
  --num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 20 \
--image-version 1.2