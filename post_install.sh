#!/bin/sh

python -m pip install "git+https://github.com/AllenNeuralDynamics/ome-zarr-py@feature/delayed-dask-poc#egg=ome-zarr"
python -m pip install "git+https://github.com/AllenNeuralDynamics/aicsimageio@zarrwriter-translation-metadata#egg=aicsimageio"
python -m pip install "git+https://github.com/AllenNeuralDynamics/aind-ng-link@feat-zarr-checker#egg=aind-ng-link"
python -m pip install hdf5plugin --no-binary hdf5plugin
