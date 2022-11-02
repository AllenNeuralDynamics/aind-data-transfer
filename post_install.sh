#!/bin/sh

python -m pip install "git+https://github.com/camilolaiton/ome-zarr-py@feature/delayed-dask-poc#egg=ome-zarr"
python -m pip install "git+https://github.com/carshadi/aicsimageio@zarrwriter-translation-metadata#egg=aicsimageio"
python -m pip install hdf5plugin --no-binary hdf5plugin
