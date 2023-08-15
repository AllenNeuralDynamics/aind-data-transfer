#!/bin/bash

SIF_VERSION="${AIND_DATA_TRANSFER_VERSION//./_}"
echo "AIND_DATA_TRANSFER_VERSION=${AIND_DATA_TRANSFER_VERSION}"
echo "SIF_VERSION=${SIF_VERSION}"

if [[ -z "${SIF_VERSION}" ]]; then
  echo "Using latest version"
  singularity build build/aind_data_transfer_latest.sif aind_data_transfer.def
  chmod 775 build/aind_data_transfer_latest.sif
else
  echo "Using ${SIF_VERSION}"
  sed -i 's/aind-data-transfer\[full\]/aind-data-transfer\[full\]=='${AIND_DATA_TRANSFER_VERSION}'/g' aind_data_transfer.def
  cat aind_data_transfer.def | grep aind-data-transfer
  singularity build build/aind_data_transfer_${SIF_VERSION}.sif aind_data_transfer.def
  chmod 775 build/aind_data_transfer_${SIF_VERSION}.sif
fi
