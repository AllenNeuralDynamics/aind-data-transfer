# Alternative to the bash script to run the main openephys job
import os

import shutil
from pathlib import Path

from aind_data_transfer.jobs.openephys_job import run_job

# ADD TUPLES OF PATH STRINGS TO RAW DATA DIRECTORY AND BEHAVIOR DIRECTORY
# SET BEHAVIOR DIR TO NONE IF INCLUDED IN RAW DATA DIRECTORY (LEGACY)
LIST_OF_DIRECTORIES_TO_PROCESS = (
    [(f"${str(Path(os.getcwd()))}/tests/resources/"
      f"v0.6.x_neuropixels_multiexp_multistream", None)]
)

# DELETE TEMP COMPRESSED DATA FOLDER AFTER UPLOAD? SET TO FALSE FOR TESTING.
DELETE_TEMP_COMPRESSED_DATA_FOLDER = True

# Path to the default configs. Change it to point to your config file.
# The contents of config file are not expected to change too often.
CONFIG_LOCATION = str(
    Path(os.getcwd())
    / "tests"
    / "resources"
    / "test_configs"
    / "ephys_upload_job_test_configs.yml"
)


if __name__ == "__main__":
    for list_item in LIST_OF_DIRECTORIES_TO_PROCESS:
        raw_data_source = list_item[0]
        behavior_dir = list_item[1]
        if behavior_dir is not None:
            dest_data_dir = run_job(["-c",
                                     CONFIG_LOCATION,
                                     "-r",
                                     raw_data_source,
                                     "-b",
                                     behavior_dir])
        else:
            dest_data_dir = run_job(["-c",
                                     CONFIG_LOCATION,
                                     "-r",
                                     raw_data_source])
        if DELETE_TEMP_COMPRESSED_DATA_FOLDER:
            shutil.rmtree(dest_data_dir)
