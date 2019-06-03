bq mk -t --schema schema.json  data_analysis.flights_delays_csv &&

bq load --source_format=CSV data_analysis.flights_delays_csv gs://sid-etl/2019-04-29.csv
