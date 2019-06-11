#!/bin/sh

FUNCTION="update_user_cart"
BUCKET="gs://your_bucket_name"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
