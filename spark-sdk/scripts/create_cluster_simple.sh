#/bin/bash

set -euxo pipefail

CLUSTER_NAME=dataproc-cluster-dev-hudi
STORAGE_PATH=gs://temp_shared_bucket/${CLUSTER_NAME}
INIT_ACTIONS_REPO=${STORAGE_PATH}/dataproc-init-actions

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway \
    --region us-west1 \
    --zone us-west1-a \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers 12 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 500 \
    --image-version 2.0.52-debian10 \
    --optional-components JUPYTER,HUDI,PRESTO \
    --initialization-actions \
gs://goog-dataproc-initialization-actions-us-west1/connectors/connectors.sh,\
gs://goog-dataproc-initialization-actions-us-west1/livy/livy.sh,\
gs://dataproc-initialization-actions/python/pip-install.sh \
    --metadata spark-bigquery-connector-version=0.26.0 \
    --metadata init-actions-repo=${INIT_ACTIONS_REPO} \
    --initialization-action-timeout=45m \
    --metadata PIP_PACKAGES="numpy scipy pandas scikit-learn matplotlib seaborn graphframes" \
    --project mvp-infra \
    --properties "^@^spark:spark.driver.maxResultSize=4096m@spark:spark.jars=gs://shared_cluster_stage/libs/graphframes-0.8.0-spark3.0-s_2.12.jar,gs://shared_cluster_stage/libs/scala-logging-api_2.11-2.1.2.jar,gs://shared_cluster_stage/libs/scala-logging-slf4j_2.11-2.1.2.jar"
