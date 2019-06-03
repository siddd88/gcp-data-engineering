gcloud dataproc clusters create spark-dwh \
 --scopes=default \
 --region "us-east1" --zone "us-east1-b" \
 --initialization-actions=gs://dataproc-initialization-actions/jupyter/jupyter.sh \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 200 \
  --num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 200 \
--image-version 1.3