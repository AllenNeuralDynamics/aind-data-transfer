#!/bin/sh

CONFIG_LOCATION=$PWD/conf/ephys_upload_job_configs.yml
# TEST_CONFIG_LOCATION=$PWD/tests/resources/ephys_upload_job_test_configs.yml

python -m transfer.jobs.openephys_job -c $CONFIG_LOCATION
