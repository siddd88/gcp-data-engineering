bq mk -t \
--schema flight_delays_table.json \
--time_partitioning_field flight_date data_analysis.flight_delays 