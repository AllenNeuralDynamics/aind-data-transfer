import fnmatch
import json

import dask.array
import numpy as np
import zarr
from distributed import Client
from numcodecs import GZip
from transfer.util.file_utils import *
from transfer.util.io_utils import DataReaderFactory
from transfer.transcode.ome_zarr import _get_or_create_pyramid


def get_downscale_factors(n_levels, scale_factor=2):
    return[ [scale_factor ** i, ] * 3 for i in range(n_levels) ]


def get_images(image_folder, exclude=None):
    if exclude is None:
        exclude = []
    image_paths = collect_filepaths(
        image_folder,
        recursive=False,
        include_exts=DataReaderFactory().VALID_EXTENSIONS,
    )

    exclude_paths = set()
    for path in image_paths:
        if any(fnmatch.fnmatch(path, pattern) for pattern in exclude):
            exclude_paths.add(path)

    image_paths = [p for p in image_paths if p not in exclude_paths]

    return image_paths


def main():
    imdir = r"C:\Users\cameron.arshadi\Downloads\Grid1"
    images = get_images(imdir)
    print(len(images))

    meta = {}

    store = zarr.N5Store(r"C:\Users\cameron.arshadi\Downloads\Grid1\n5_test\dataset.n5")
    root = zarr.group(store, overwrite=True)

    n_scales = 2

    factors = get_downscale_factors(n_scales, scale_factor=2)
    print(factors)

    client = Client()

    for i, impath in enumerate(images):

        reader = DataReaderFactory().create(impath)
        shape = reader.get_shape()

        setup = root.create_group(f"setup{i}")
        setup.attrs["downsamplingFactors"] = factors
        setup.attrs['dataType'] = "uint16"

        timepoint = setup.create_group("timepoint0")
        timepoint.attrs['resolution'] = [1.0, 1.0, 1.0]
        timepoint.attrs['saved_completely'] = True
        timepoint.attrs['multiScale'] = True

        im_meta = {}
        im_meta['tileName'] = Path(impath).name
        im_meta['voxelSize'] = {
            "size": [1.0, 1.0, 1.0],  # X Y Z
            "unit": "pixels",
        }
        im_meta['id'] = i
        im_meta['name'] = i
        im_meta['size'] = tuple(int(d) for d in np.flip(shape))  # X Y Z
        im_meta['origin'] = [0, 0, 0]  # X Y Z
        im_meta['attributes'] = {
            "illumination": 0,
            "channel": 0,
            "tile": i,
            "angle": 0
        }

        print(im_meta)

        meta[Path(impath).name] = im_meta

        chunks = (64,128,128)

        pyramid = _get_or_create_pyramid(reader, n_scales, chunks=chunks)
        pyramid = [p.astype(np.uint16) for p in pyramid]
        # pyramid = [p.rechunk(chunks) for p in pyramid]
        print(pyramid[0].chunks)
        #futures = []
        for i, arr in enumerate(pyramid):
            path = f"s{i}"
            dask.array.to_zarr(
                array_key=path,
                arr=arr,
                url=timepoint.store,
                component=str(Path(timepoint.path, str(path))),
                overwrite=True,
                compute=True,
                compressor=GZip(5),
            )
            timepoint[path].attrs['downsamplingFactors'] = factors[i]

            #futures.append(fut)

        # jobs = dask.persist(*futures)
        # progress(jobs)

    with open(r"C:\Users\cameron.arshadi\Downloads\Grid1\n5_test\meta.json", 'w') as f:
        json.dump(meta, f, indent=4)


if __name__ == "__main__":
    main()