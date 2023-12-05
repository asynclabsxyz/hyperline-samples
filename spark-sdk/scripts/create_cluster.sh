#/bin/bash

set -euxo pipefail

INIT_ACTIONS_REPO=gs://temp_shared_bucket/stanislav@asynclabs.xyz/dataproc-init-actions

gcloud dataproc clusters create test-cluster-15 \
    --enable-component-gateway \
    --region us-west1 \
    --zone us-west1-a \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 500 \
    --master-accelerator type=nvidia-tesla-t4,count=2 \
    --worker-accelerator type=nvidia-tesla-t4,count=2 \
    --image-version 2.0-ubuntu18 \
    --optional-components JUPYTER \
    --max-idle 10800s \
    --initialization-actions \
gs://goog-dataproc-initialization-actions-us-west1/connectors/connectors.sh,\
gs://temp_shared_bucket/stanislav@asynclabs.xyz/init-actions/hyperline/mlvm.sh,\
gs://temp_shared_bucket/stanislav@asynclabs.xyz/init-actions/hyperline/hyperline.sh \
    --metadata spark-bigquery-connector-version=0.26.0 \
    --metadata rapids-runtime=SPARK \
    --metadata include-gpus=true \
    --metadata gpu-driver-provider=NVIDIA \
    --metadata init-actions-repo=${INIT_ACTIONS_REPO} \
    --metadata cuda-version=11.2 \
    --metadata cudnn-version=8.1.1.33 \
    --initialization-action-timeout=45m \
    --project mvp-infra
