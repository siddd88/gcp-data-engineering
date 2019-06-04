#!/bin/bash

bucket="gs://sidd-etl"
instance_name="bigdata-etl-240212:us-central1:mysql-instance"
table_name="flights"
target_dir="$bucket/sqoop_output/$table_name"


#Simple table import 

# gcloud dataproc jobs submit hadoop \
# --cluster=$1 --region asia-east1 \
# --class=org.apache.sqoop.Sqoop \
# --jars=$bucket/sqoop_jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop_jars/sqoop_avro-tools-1.8.2.jar,\
# file:///usr/share/java/mysql-connector-java-5.1.42.jar \
# -- import -Dmapreduce.job.user.classpath.first=true \
# --driver com.mysql.jdbc.Driver \
# --connect="jdbc:mysql://localhost:3307/airports" \
# --username=root --password=Siddharth88!@ \
# --split-by id \
# --table flights \
# --split-by id -m 4 \
# --target-dir gs://sidd-etl/sqoop_output/ 

#  gcloud dataproc jobs submit hadoop \
# --cluster=$cluster_name --region global \
# --class=org.apache.sqoop.Sqoop \
# --jars=$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,\
# file:///usr/share/java/mysql-connector-java-5.1.42.jar \
# -- import -Dmapreduce.job.user.classpath.first=true \
# --driver com.mysql.jdbc.Driver \
# --connect="jdbc:mysql://localhost:3307" \
# --username=root --password=Siddharth88!@ \
# --query "select * from airports.flights where 1=1 and \$CONDITIONS limit 100" \
# --target-dir $target_dir \
# --split-by id -m 2

# gcloud dataproc jobs submit hadoop \
# --cluster=$cluster_name --region global \
# --class=org.apache.sqoop.Sqoop \
# --jars=$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,\
# file:///usr/share/java/mysql-connector-java-5.1.42.jar \
# -- import -Dmapreduce.job.user.classpath.first=true \
# --driver com.mysql.jdbc.Driver \
# --connect="jdbc:mysql://localhost:3307" \
# --username=root --password=Siddharth88!@ \
# --query "select * from airports.flights where 1=1 \$CONDITIONS limit 100" \
# --warehouse-dir $target_dir \
# --split-by id -m 2