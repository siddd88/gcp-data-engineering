#!/bin/bash

bucket="gs://bucket_name"

pwd_file=$bucket/sqoop-pwd/pwd.txt

cluster_name="ephemeral-spark-cluster-20190519"

table_name="airports"

target_dir=$bucket/sqoop_output


# Simple table import - Avro Format - No Primary Key 

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region=asia-east1 \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.classloader=true \
-Dmapreduce.output.basename="part.airports_" \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--m 1 \
--table $table_name \
--warehouse-dir $target_dir \
--as-avrodatafile 