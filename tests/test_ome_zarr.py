import math
import os
import tempfile
import unittest
from pathlib import Path

import h5py
import numpy as np
import tifffile
import zarr
from distributed import Client
from parameterized import parameterized

from aind_data_transfer.transcode.ome_zarr import write_files, write_folder


def _write_test_tiffs(folder, n=4, shape=(64, 128, 128)):
    for i in range(n):
        a = np.ones(shape, dtype=np.uint16)
        tifffile.imwrite(os.path.join(folder, f"data_{i}.tif"), a)


def _write_test_h5(folder, n=4, shape=(64, 128, 128)):
    DEFAULT_DATA_PATH = "/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0/Data"
    for i in range(n):
        a = np.ones(shape, dtype=np.uint16)
        with h5py.File(os.path.join(folder, f"data_{i}.h5"), 'w') as f:
            f.create_dataset(DEFAULT_DATA_PATH, data=a, chunks=True)
            # Write origin metadata
            dataset_info = f.create_group("DataSetInfo/Image")
            dataset_info.attrs["ExtMin0"] = np.array(['1', '0', '0'], dtype='S')
            dataset_info.attrs["ExtMin1"] = np.array(['2', '0', '0'], dtype='S')
            dataset_info.attrs["ExtMin2"] = np.array(['3', '0', '0'], dtype='S')


class TestOmeZarr(unittest.TestCase):
    """Tests methods defined in aind_data_transfer.transcode.ome_zarr class"""

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._image_dir = Path(self._temp_dir.name) / "images"
        os.makedirs(self._image_dir, exist_ok=True)
        self.tiff_dir = self._image_dir / "tiff"
        self.h5_dir = self._image_dir / "h5"
        os.mkdir(self.tiff_dir)
        os.mkdir(self.h5_dir)
        _write_test_tiffs(self.tiff_dir)
        _write_test_h5(self.h5_dir)
        self._client = Client()

    def tearDown(self):
        self._temp_dir.cleanup()
        self._client.close()

    def _check_multiscales_arrays(
            self, z, full_shape, actual_keys, n_levels, scale_factor
    ):
        # Test arrays across resolution levels
        for key in actual_keys:
            for lvl in range(n_levels):
                a = z[key + f"/{lvl}"]
                expected_shape_at_lvl = (
                    1,
                    1,
                    int(math.ceil(full_shape[2] / (scale_factor ** lvl))),
                    int(math.ceil(full_shape[3] / (scale_factor ** lvl))),
                    int(math.ceil(full_shape[4] / (scale_factor ** lvl))),
                )
                self.assertEqual(expected_shape_at_lvl, a.shape)
                self.assertTrue(a.nbytes_stored > 0)

    def _check_zarr_attributes(self, z, voxel_size, scale_factor, has_translation):
        for key in z.keys():
            tile_group = z[key]
            attrs = dict(tile_group.attrs)
            expected_omero_metadata = {
                "channels": [
                    {
                        "active": True,
                        "coefficient": 1,
                        "color": "000000",
                        "family": "linear",
                        "inverted": False,
                        "label": f"Channel:{key}:0",
                        "window": {
                            "end": 1.0,
                            "max": 1.0,
                            "min": 0.0,
                            "start": 0.0,
                        },
                    }
                ],
                "id": 1,
                "name": f"{key}",
                "rdefs": {"defaultT": 0, "defaultZ": 32, "model": "color"},
                "version": "0.4",  # TODO: test against version?
            }
            actual_omero_metadata = attrs["omero"]
            self.assertEqual(
                expected_omero_metadata["channels"],
                actual_omero_metadata["channels"],
            )
            self.assertEqual(
                expected_omero_metadata["id"], actual_omero_metadata["id"]
            )
            self.assertEqual(
                expected_omero_metadata["name"], actual_omero_metadata["name"]
            )
            self.assertEqual(
                expected_omero_metadata["rdefs"],
                actual_omero_metadata["rdefs"],
            )

            expected_axes_metadata = [
                {"name": "t", "type": "time", "unit": "millisecond"},
                {"name": "c", "type": "channel"},
                {"name": "z", "type": "space", "unit": "micrometer"},
                {"name": "y", "type": "space", "unit": "micrometer"},
                {"name": "x", "type": "space", "unit": "micrometer"},
            ]
            self.assertEqual(
                expected_axes_metadata, attrs["multiscales"][0]["axes"]
            )

            datasets_metadata = attrs["multiscales"][0]["datasets"]
            for i, ds_metadata in enumerate(datasets_metadata):
                expected_transform = {
                    "coordinateTransformations": [
                        {
                            "scale": [
                                1.0,
                                1.0,
                                voxel_size[0] * (scale_factor ** i),
                                voxel_size[1] * (scale_factor ** i),
                                voxel_size[2] * (scale_factor ** i),
                            ],
                            "type": "scale",
                        }
                    ],
                    "path": f"{i}",
                }
                if has_translation:
                    expected_transform['coordinateTransformations'].append({
                        "translation": [0, 0, 300.0, 200.0, 100.0],
                        "type": "translation"
                    })
                self.assertEqual(expected_transform, datasets_metadata[i])

            self.assertEqual(f"/{key}", attrs["multiscales"][0]["name"])

    @parameterized.expand(["h5", "tiff"])
    def test_write_files(self, file_type):
        image_dir = None
        if file_type == "h5":
            image_dir = self.h5_dir
        elif file_type == "tiff":
            image_dir = self.tiff_dir

        files = list(
            sorted([image_dir / f for f in image_dir.iterdir()])
        )

        shape = (64, 128, 128)
        out_zarr = os.path.join(self._temp_dir.name, "ome.zarr")
        n_levels = 4
        scale_factor = 2.0
        voxel_size = [1.0, 1.0, 1.0]
        metrics = write_files(
            files, out_zarr, n_levels, scale_factor, voxel_size=voxel_size
        )

        self.assertEqual(len(metrics), len(files))

        z = zarr.open(out_zarr, "r")

        # Test group for each written image
        expected_keys = {"data_0", "data_1", "data_2", "data_3"}
        actual_keys = set(z.keys())
        self.assertEqual(expected_keys, actual_keys)

        expected_shape = (1, 1, *shape)
        self._check_multiscales_arrays(
            z, expected_shape, actual_keys, n_levels, scale_factor
        )

        self._check_zarr_attributes(z, voxel_size, scale_factor, has_translation=(file_type == "h5"))

    @parameterized.expand(["h5", "tiff"])
    def test_write_folder(self, file_type):
        image_dir = None
        if file_type == "h5":
            image_dir = self.h5_dir
        elif file_type == "tiff":
            image_dir = self.tiff_dir

        shape = (64, 128, 128)
        out_zarr = os.path.join(self._temp_dir.name, "ome.zarr")
        n_levels = 4
        scale_factor = 2.0
        voxel_size = [1.0, 1.0, 1.0]

        # create a dummy file to exclude from conversion
        tifffile.imwrite(
            image_dir / "dummy.tif", np.zeros(shape)
        )
        exclude = ["*dummy*"]

        write_folder(
            image_dir,
            out_zarr,
            n_levels,
            scale_factor,
            voxel_size=voxel_size,
            exclude=exclude,
        )

        z = zarr.open(out_zarr, "r")

        # Test that all images were written except exclusion
        expected_keys = {"data_0", "data_1", "data_2", "data_3"}
        actual_keys = set(z.keys())
        self.assertEqual(expected_keys, actual_keys)

        expected_shape = (1, 1, *shape)
        self._check_multiscales_arrays(
            z, expected_shape, actual_keys, n_levels, scale_factor
        )

        self._check_zarr_attributes(z, voxel_size, scale_factor, has_translation=(file_type == "h5"))


if __name__ == "__main__":
    unittest.main()
