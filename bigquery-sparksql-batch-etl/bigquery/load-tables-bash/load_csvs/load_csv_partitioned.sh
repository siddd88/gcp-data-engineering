bq mk -t \
--schema schema.json \
--time_partitioning_field flight_date data_analysis.flights_delays_csv_partitioned && 

bq load --source_format=CSV data_analysis.flights_delays_csv_partitioned gs://your_bucket_name/2019-04-27.csv
