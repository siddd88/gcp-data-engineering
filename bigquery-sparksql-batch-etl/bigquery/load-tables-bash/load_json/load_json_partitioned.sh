bq mk -t \
--schema schema.json \
--time_partitioning_field flight_date data_analysis.flight_delays_json_partitioned && 

bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.flight_delays_json_partitioned \
 gs://sid-etl/2019-04-27.json


bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.flight_delays_json_partitioned \
 gs://sid-etl/2019-04-28.json


 bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
 data_analysis.flight_delays_json_partitioned \
 gs://sid-etl/2019-04-27.json
