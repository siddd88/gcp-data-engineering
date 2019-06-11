#!/bin/bash

bucket="gs://bucket_name"
instance_name="your_cloudsql_instance_name"
table_name="flights"

target_dir="/incremental_appends"

pwd_file=$bucket/sqoop-pwd/pwd.txt
cluster_name="ephemeral-spark-cluster-20190519"


gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region=asia-east1 \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.user.classpath.first=true \
-Dmapreduce.output.basename="part.20190514_" \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--split-by id \
--table $table_name \
--check-column flight_date \
--last-value 2019-05-13 \
--incremental append \
-m 4 \
--target-dir $target_dir \
--as-avrodatafile 


# gcloud compute ssh ephemeral-spark-cluster-20190518-m \
# --zone='asia-east1-a' \
# -- -T 'hadoop distcp /incremental_appends/*.avro gs://sidd-etl/incremental_appends'
