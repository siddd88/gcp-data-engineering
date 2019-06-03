bucket=gs://sid-etl

current_date=$(date +"%Y-%m-%d")


bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_distance \
 $bucket/flights_data_output/${current_date}"_distance_category/*.json" &&


bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_flight_nums \
 $bucket/flights_data_output/${current_date}"_flight_nums/*.json"
