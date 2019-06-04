#!/bin/bash

current_date=$1
bucket="gs://sidd-etl"
cluster_name="sqoop-hdp"
instance_name="bigdata-etl-240212:us-central1:mysql-instance"
table_name="flights"
target_dir="/sqoop_output"

#Simple table import  : -Dmapreduce.job.user.classpath.first=true

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region global \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,\
file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.classloader=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password=$bucket/sqoop-pwd/pwd.txt \
--split-by id \
--table flights -m 4 \
--check-column flight_date \
--incremental append --merge-key flight_date \
--last-value 2014-04-13 \
--target-dir $target_dir --as-avrodatafile 


# gcloud compute ssh ephemeral-spark-cluster-20190518-m \
# --zone='asia-east1-a' \
# -- -T 'hadoop distcp /incremental_appends/*.avro gs://sidd-etl/incremental_appends'