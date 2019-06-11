#!/bin/sh

FUNCTION="iot_transformed_data"
BUCKET="gs://sidd_streaming/"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
