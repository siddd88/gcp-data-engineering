#!/bin/sh

FUNCTION="update_user_cart"
BUCKET="gs://sidd-streams/"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
