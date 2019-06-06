#!/bin/sh

FUNCTION="update_visits_by_categories"
BUCKET="gs://sidd-visits-analysis/"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
