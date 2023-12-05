#!/bin/bash

set -euxo pipefail

readonly SPARK_JARS_DIR=/usr/lib/spark/jars
readonly CONNECTORS_DIR=/usr/local/share/google/dataproc/lib

readonly DEFAULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/init-actions-repo ||
  echo ${DEFAULT_INIT_ACTIONS_REPO})"
readonly INIT_ACTIONS_DIR=$(mktemp -d -t dataproc-init-actions-XXXX)

readonly RAPIDS_RUNTIME="$(/usr/share/google/get_metadata_value attributes/rapids-runtime || echo "")"
readonly INCLUDE_GPUS="$(/usr/share/google/get_metadata_value attributes/include-gpus || echo "")"
readonly SPARK_BIGQUERY_VERSION="$(/usr/share/google/get_metadata_value attributes/spark-bigquery-connector-version ||
  echo "0.22.0")"

readonly SPARK_NLP_VERSION="3.2.1" # Must include subminor version here

PIP_PACKAGES=(
  "mxnet==1.8.*"
  "rpy2==3.4.*"
  "spark-nlp==${SPARK_NLP_VERSION}"
  "sparksql-magic==0.0.*"
  "tensorflow-datasets==4.4.*"
  "tensorflow-hub==0.12.*"
)

PIP_PACKAGES+=(
  "spark-tensorflow-distributor==1.0.0"
  "tensorflow==2.6.*"
  "tensorflow-estimator==2.6.*"
  "tensorflow-io==0.20"
  "tensorflow-probability==0.13.*"
)

readonly PIP_PACKAGES

mkdir -p ${SPARK_JARS_DIR} ${CONNECTORS_DIR}

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  echo "Cmd '${cmd}' failed."
  return 1
}

function download_spark_jar() {
  local -r url=$1
  local -r jar_name=${url##*/}
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${url}" -o "${SPARK_JARS_DIR}/${jar_name}"
}

function download_init_actions() {
  # Download initialization actions locally.
  mkdir "${INIT_ACTIONS_DIR}"/{gpu,rapids,dask}

  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/rapids/" "${INIT_ACTIONS_DIR}/rapids/"
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/gpu/" "${INIT_ACTIONS_DIR}/gpu/"
  gsutil -m rsync -r "${INIT_ACTIONS_REPO}/dask/" "${INIT_ACTIONS_DIR}/dask/"

  find "${INIT_ACTIONS_DIR}" -name '*.sh' -exec chmod +x {} \;
}

function install_gpu_drivers() {
  "${INIT_ACTIONS_DIR}/gpu/install_gpu_driver.sh"
}

function install_pip_packages() {
  local -r extra_packages="$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || echo "")"

  execute_with_retries "pip install ${PIP_PACKAGES[*]}"

  if [[ -n "${extra_packages}" ]]; then
    execute_with_retries "pip install ${extra_packages[*]}"
  fi

  execute_with_retries "pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu"
}

function install_dask() {
  "${INIT_ACTIONS_DIR}/dask/dask.sh"
}

function install_spark_nlp() {
  local -r name="spark-nlp_2.12"
  local -r repo_url="https://repo1.maven.org/maven2/com/johnsnowlabs/nlp"
  download_spark_jar "${repo_url}/${name}/${SPARK_NLP_VERSION}/${name}-${SPARK_NLP_VERSION}.jar"
}

function install_connectors() {
  local -r url="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-${SPARK_BIGQUERY_VERSION}.jar"

  gsutil cp "${url}" "${CONNECTORS_DIR}/"

  local -r jar_name=${url##*/}

  # Update or create version-less connector link
  ln -s -f "${CONNECTORS_DIR}/${jar_name}" "${CONNECTORS_DIR}/spark-bigquery-connector.jar"
}

function install_rapids() {
  # Only install RAPIDS if "rapids-runtime" metadata exists and GPUs requested.
  if [[ -n ${RAPIDS_RUNTIME} ]]; then
    if [[ -n ${INCLUDE_GPUS} ]]; then
      "${INIT_ACTIONS_DIR}/rapids/rapids.sh"
    else
      echo "RAPIDS runtime declared but GPUs not included. Exiting."
      return 1
    fi
  fi
}

function main() {
  # Download initialization actions
  echo "Downloading initialization actions"
  download_init_actions

  # Install GPU Drivers
  echo "Installing GPU drivers"
  install_gpu_drivers

  # Install Dask
  install_dask

  # Install Spark Libraries
  echo "Installing Spark-NLP jars"
  install_spark_nlp

  # Install GCP Connectors
  echo "Installing GCP Connectors"
  install_connectors

  # Install RAPIDS
  echo "Installing rapids"
  install_rapids

  # Install Pip packages
  echo "Installing Pip Packages"
  install_pip_packages
}

main