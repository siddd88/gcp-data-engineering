bq mk -t \
--schema schema/avg_delay_flight_nums.json \
--time_partitioning_field flight_date data_analysis.avg_delays_by_flight_nums                                                                      &&


bq mk -t \
--schema schema/avg_delays_by_distance.json \
--time_partitioning_field flight_date data_analysis.avg_delays_by_distance