#!/bin/sh

TEST_CONFIG_LOCATION=$PWD/conf/transcode_job_configs.yml

# The job script traverses parent directories to find the scripts/ dir, which
# is why I'm not running as a module here
python $PWD/src/aind_data_transfer/jobs/transcode_job.py -c $TEST_CONFIG_LOCATION
