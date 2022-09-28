#!/bin/sh

# TODO: Move this to a config file?
UNCOMPRESSED_DATA_DIR=$PWD'/tests/resources/v0.6.x_neuropixels_multiexp_multistream'
DIR_FOR_COMPRESSED_DATA=$PWD'/tests/resources/compressed_data'
# Is it too late to convert the dashes to underscores?
PREFIX='/ecephys_123456_'
DATETIME_STRING=$(date +'%Y_%m_%d_%H-%M-%S')

AWS_BUCKET='aind-transfer-test'
AWS_LOCATION_FOR_UNCOMPRESSED_DATA='s3://'$AWS_BUCKET$PREFIX$DATETIME_STRING'/ecephys/v0.6.x_neuropixels_multiexp_multistream/'
AWS_LOCATION_FOR_COMPRESSED_DATA='s3://'$AWS_BUCKET$PREFIX$DATETIME_STRING'/ecephys_compressed/'

READER_CONFIGS='{"reader_name":"openephys","input_dir":"'$UNCOMPRESSED_DATA_DIR'"}'
COMPRESSOR_CONFIGS='{"compressor_name":"wavpack","kwargs":{"level":3}}'
WRITER_CONFIGS='{"output_dir":"'$DIR_FOR_COMPRESSED_DATA'","job_kwargs":{"n_jobs":20,"chunk_duration":"1s","progress_bar":true}}'

python -m transfer.jobs.open_ephys_job -r $READER_CONFIGS -c $COMPRESSOR_CONFIGS -w $WRITER_CONFIGS

aws s3 sync $UNCOMPRESSED_DATA_DIR $AWS_LOCATION_FOR_UNCOMPRESSED_DATA --exclude "*.dat"
aws s3 sync $DIR_FOR_COMPRESSED_DATA $AWS_LOCATION_FOR_COMPRESSED_DATA
