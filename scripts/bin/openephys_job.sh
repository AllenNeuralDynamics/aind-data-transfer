#!/bin/sh

TEST_CONFIG_LOCATION=$PWD/tests/resources/test_configs/ephys_upload_job_test_configs.yml

python -m transfer.jobs.openephys_job -c $TEST_CONFIG_LOCATION
