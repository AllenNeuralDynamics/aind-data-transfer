# Alternative to the bash script to run the main openephys job
import os

# import shutil
from pathlib import Path

from aind_data_transfer.jobs.openephys_job import run_job

# Path to the default configs. Change it to point to your config file.
# The contents of config file are not expected to change too often.
CONFIG_LOCATION = str(
    Path(os.getcwd())
    / "tests"
    / "resources"
    / "test_configs"
    / "ephys_upload_job_test_configs.yml"
)

# Paths to the raw data source and video directory. You can wrap things in a
# for loop if multiple data sets need to be processed.

# You can replace the right-hand side with a hard-coded string of the absolute
# path to the raw data.
RAW_DATA_SOURCE = str(
    Path(os.getcwd())
    / "tests"
    / "resources"
    / "v0.6.x_neuropixels_multiexp_multistream"
)

# Optionally point to a video directory if it's not bundled with the raw data.
VIDEO_DIR = None

if VIDEO_DIR is not None:
    run_job(["-c", CONFIG_LOCATION, "-r", RAW_DATA_SOURCE, "-v", VIDEO_DIR])
else:
    run_job(["-c", CONFIG_LOCATION, "-r", RAW_DATA_SOURCE])

# Add a cleanup script to remove the local compressed data folder if desired.
# PATH_TO_DEST_FOLDER = Path("")
# shutil.rmtree(PATH_TO_DEST_FOLDER)
