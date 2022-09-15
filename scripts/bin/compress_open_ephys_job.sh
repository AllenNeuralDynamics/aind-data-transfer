#!/bin/sh

READER_CONFIGS='{"reader_name":"openephys","input_dir":"'$PWD'/tests/resources/v0.6.x_neuropixels_multiexp_multistream"}'
COMPRESSOR_CONFIGS='{"compressor_name":"wavpack","kwargs":{"level":3}}'
WRITER_CONFIGS='{"output_dir":"'$PWD'/tests/resources/zarr_stuff","job_kwargs":{"n_jobs":20,"chunk_duration":"1s","progress_bar":true}}'

python -m transfer.jobs.open_ephys_job -r $READER_CONFIGS -c $COMPRESSOR_CONFIGS -w $WRITER_CONFIGS