# Define the variable 
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name) 


/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic user_browsing_logs

# Command to create a kafka topic 
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 \
--create \
--replication-factor 2 --partitions 4 --topic user_logs



tail -f server-logs/server_* | \
/usr/lib/kafka/bin/kafka-console-producer.sh --broker-list ${CLUSTER_NAME}-w-0:9092 --topic user_browsing_logs &

/usr/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --topic user_browsing_logs --from-beginning


/usr/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --topic high_consumption_alerts --from-beginning
