#!/bin/bash

python -m pip install "git+https://github.com/AllenNeuralDynamics/aind-ng-link@feat-zarr-checker#egg=aind-ng-link" --no-cache-dir
python -m pip install "git+https://github.com/fsspec/kerchunk" --no-cache-dir
python -m pip install hdf5plugin --no-binary hdf5plugin --no-cache-dir
