import os

import yaml
from yaml import Loader

_HOME = os.getenv("HOME")
# This is where dask-jobqueue looks for the cluster configuration
DASK_CONF_FILE = f"{_HOME}/.config/dask/dask-config.yaml"


def load_dask_config():
    with open(DASK_CONF_FILE, "r") as f:
        config = yaml.load(f, Loader)
    return config
