#!/bin/sh

echo "Building directory and getting package version"
mkdir -p build
AIND_DATA_TRANSFER_VERSION=$(grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' src/aind_data_transfer/__init__.py)
echo ${AIND_DATA_TRANSFER_VERSION}

echo "Building docker container"
docker build -t singularity-builder:latest --build-arg AIND_DATA_TRANSFER_VERSION=latest -f scripts/singularity/Dockerfile .

echo "Building sif container"
docker run --privileged -e AIND_DATA_TRANSFER_VERSION=${AIND_DATA_TRANSFER_VERSION} -v ${PWD}/build:/build singularity-builder:latest ./build_sif.sh
