"""Import avro file into BigQuery."""

import os 

from google.cloud import bigquery

def iot_transformed_data(data,context) :

    client = bigquery.Client()

    bucketname = data['bucket']

    timeCreated = data['timeCreated']

    dataset_id = "streaming_data_analysis"

    dataset_ref = client.dataset(dataset_id)

    job_config = bigquery.LoadJobConfig()

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config.autodetect = True 

    job_config.ignore_unknown_values = True

    job_config.source_format = bigquery.SourceFormat.AVRO

    uri = bucketname+"/iot_transformed_data/*.avro"

    load_job = client.load_table_from_uri(uri,dataset_ref.table('device_energy_consumption'),job_config=job_config)

    print("Starting Job {}".format(load_job.job_id))

    load_job.result()

    print("Job Finished")

    destination_table = client.get_table(dataset_ref.table('device_energy_consumption'))
    print('Loaded {} rows.' .format(destination_table.num_rows))