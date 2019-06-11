#!/bin/bash

bucket="gs://your_bucket_name"
template_name="mysql-flights-import"
cluster_name="your_cluster_name"
instance_name="your_cloudsql_instance_name"
table_name="flights"

$pwd_file="gs://your_bucket_name/pwd_directory/pwd.txt"

#gsutil rm -r $bucket/$table_name && 

gcloud dataproc workflow-templates delete -q $template_name  &&

gcloud beta dataproc workflow-templates create $template_name &&

gcloud beta dataproc workflow-templates set-managed-cluster $template_name --zone "asia-east1-a" \
--cluster-name=$cluster_name \
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
--image-version 1.2 &&

gcloud beta dataproc workflow-templates add-job hadoop \
--step-id=flights_test_data \
--workflow-template=$template_name \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,\
file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import -Dmapreduce.job.user.classpath.first=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--table flights \
--split-by id -m 4 \
--delete-target-dir \
--target-dir $bucket/sqoop_output/ &&

gcloud beta dataproc workflow-templates instantiate $template_name
