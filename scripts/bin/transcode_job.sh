#!/bin/sh

TEST_CONFIG_LOCATION=$PWD/conf/transcode_job_configs.yml
RAW_DATA_SOURCE=/net/172.20.102.30/aind/exaSPIM/exaSPIM_614978_stitching_test_2022-10-25_12-49-57

# The job script traverses parent directories to find the scripts/ dir, which
# is why I'm not running as a module here
python $PWD/src/aind_data_transfer/jobs/transcode_job.py -c $TEST_CONFIG_LOCATION -r $RAW_DATA_SOURCE
