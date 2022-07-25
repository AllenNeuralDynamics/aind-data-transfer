import math
import os
import time
import numpy as np
from aicsimageio.writers import OmeZarrWriter
import dask_image.imread
from distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
from numcodecs import blosc
#blosc.use_threads = False
from tifffile import tifffile


def main():

    local_directory = "/scratch/fast/$SLURM_JOB_ID"
    log_directory = "/home/cameron.arshadi/.dask_distributed/dask-worker-logs"

    cluster = SLURMCluster(cores=1, memory="4GB", queue='aind', local_directory=local_directory,
                           log_directory=log_directory)
    n_jobs = 32
    cluster.scale(n_jobs)

    #cluster = LocalCluster(processes=True)

    client = Client(cluster)

    filepath = "/net/172.20.102.30/aind/mesospim_ANM457202_2022_07_11/micr/tile_0_0.tiff"

    tile_bytes = os.stat(filepath).st_size
    print("tile size ", tile_bytes / (1024**2), 'MB')

    with tifffile.TiffFile(filepath) as tiff:
        print(tiff.flags)
        print(len(tiff.pages))
        print(tiff.pages[0].is_tiled)
        print(tiff.pages[0].shape)

    data = dask_image.imread.imread(filepath)
    # Force 3D Tile to TCZYX
    data = data[np.newaxis, np.newaxis, ...]
    print(data)

    compressor = blosc.Blosc(cname="zstd", clevel=1, shuffle=blosc.SHUFFLE)

    opts = {
        "compressor": compressor,
    }

    mypath = "/allen/programs/aind/workgroups/msma/cameron.arshadi/ome-zarr-test/ome-zarr-tile-test.zarr"
    #mypath = "s3://aind-transfer-test/ome-zarr-test/ome-zarr-tile-test.zarr"

    writer = OmeZarrWriter(mypath)

    t0 = time.time()
    writer.write_image(
        image_data=data,  # : types.ArrayLike,  # must be 5D TCZYX
        image_name='tile-test',  #: str,
        physical_pixel_sizes=None,
        channel_names=None,
        channel_colors=None,
        scale_num_levels=1,  # : int = 1,
        scale_factor=2.0,  # : float = 2.0,
        target_chunk_size=256,  # MB
        storage_options=opts
    )
    write_time = time.time() - t0
    print(f"Done. Took {write_time}s. {tile_bytes / write_time / (1024 ** 2)} MiB/s")


if __name__ == "__main__":
    main()
