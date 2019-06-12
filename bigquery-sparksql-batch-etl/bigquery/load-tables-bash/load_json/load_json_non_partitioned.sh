bq mk -t \
--schema schema.json \
data_analysis.flight_delays_json_non_partitioned && 


bq load --source_format=NEWLINE_DELIMITED_JSON  \
 data_analysis.flight_delays_json_non_partitioned \
 gs://your_bucket_name/2019-04-27.json



 bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
 data_analysis.flight_delays_json_non_partitioned \
 gs://your_bucket_name/2019-04-27.json