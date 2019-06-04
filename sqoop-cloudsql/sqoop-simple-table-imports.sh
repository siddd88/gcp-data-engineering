#!/bin/bash

current_date=$1
bucket="gs://sidd-etl"
cluster_name="ephemeral-spark-cluster-20190518"
instance_name="bigdata-etl-240212:us-central1:mysql-instance"
table_name="flights"
target_dir=$bucket/

#Simple table import - Text Format : 

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region asia-east1 \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,\
file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.user.classpath.first=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$bucket/sqoop-pwd/pwd.txt \
--split-by id \
--table flights \
 -m 4 \
--target-dir $target_dir 

#Simple table import - Avro Format : 
# gcloud dataproc jobs submit hadoop \
# --cluster=$cluster_name --region asia-east1 \
# --class=org.apache.sqoop.Sqoop \
# --jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,\
# file:///usr/share/java/mysql-connector-java-5.1.42.jar \
# -- import \
# -Dmapreduce.job.classloader=true \
# --driver com.mysql.jdbc.Driver \
# --connect="jdbc:mysql://localhost:3307/airports" \
# --username=root --password-file=$bucket/sqoop-pwd/pwd.txt \
# --split-by id \
# --table flights \
#  -m 4 \
# --target-dir $target_dir --as-avrodatafile