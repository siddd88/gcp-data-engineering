#!/bin/sh

FUNCTION="hive_transformed_data"
BUCKET="gs://your_bucket_name"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
