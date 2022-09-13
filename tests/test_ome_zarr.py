import math
import os
import tempfile
import unittest

import numpy as np
import tifffile
import zarr
from distributed import Client
from transfer.transcode.ome_zarr import write_files


def _write_test_tiffs(folder, n=4, shape=(64, 128, 128)):
    for i in range(n):
        a = np.ones(shape, dtype=np.uint16)
        tifffile.imwrite(os.path.join(folder, f"data_{i}.tif"), a)


class TestOmeZarr(unittest.TestCase):
    """Tests methods defined in transfer.transcode.ome_zarr class"""

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        _write_test_tiffs(self._temp_dir.name)
        self._client = Client()

    def tearDown(self):
        self._temp_dir.cleanup()
        self._client.close()

    def test_write_files(self):
        files = list(sorted([os.path.join(self._temp_dir.name, f) for f in os.listdir(self._temp_dir.name)]))
        out_zarr = os.path.join(self._temp_dir.name, "ome.zarr")
        n_levels = 4
        scale_factor = 2.0
        voxel_size = [1.0, 1.0, 1.0]
        metrics = write_files(files, out_zarr, n_levels, scale_factor, voxel_size=voxel_size)

        self.assertEqual(len(metrics), len(files))

        z = zarr.open(out_zarr, 'r')

        # Test group for each written image
        expected_keys = {"data_0", "data_1", "data_2", "data_3"}
        actual_keys = set(z.keys())
        self.assertEqual(expected_keys, actual_keys)

        # Test arrays across resolution levels
        expected_shape = (1, 1, 64, 128, 128)
        for key in actual_keys:
            for lvl in range(n_levels):
                a = z[key + f'/{lvl}']
                expected_shape_at_lvl = (
                    1,
                    1,
                    int(math.ceil(expected_shape[2] / (scale_factor ** lvl))),
                    int(math.ceil(expected_shape[3] / (scale_factor ** lvl))),
                    int(math.ceil(expected_shape[4] / (scale_factor ** lvl))),
                )
                self.assertEqual(expected_shape_at_lvl, a.shape)
                self.assertTrue(a.nbytes_stored > 0)

        # zarr attributes tests

        for key in actual_keys:
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
                            "start": 0.0
                        }
                    }
                ],
                "id": 1,
                "name": f"{key}",
                "rdefs": {
                    "defaultT": 0,
                    "defaultZ": 32,
                    "model": "color"
                },
                "version": "0.4"  # TODO: test against version?
            }
            actual_omero_metadata = attrs['omero']
            self.assertEqual(expected_omero_metadata['channels'], actual_omero_metadata['channels'])
            self.assertEqual(expected_omero_metadata['id'], actual_omero_metadata['id'])
            self.assertEqual(expected_omero_metadata['name'], actual_omero_metadata['name'])
            self.assertEqual(expected_omero_metadata['rdefs'], actual_omero_metadata['rdefs'])

            expected_axes_metadata = [
                {
                    "name": "t",
                    "type": "time",
                    "unit": "millisecond"
                },
                {
                    "name": "c",
                    "type": "channel"
                },
                {
                    "name": "z",
                    "type": "space",
                    "unit": "micrometer"
                },
                {
                    "name": "y",
                    "type": "space",
                    "unit": "micrometer"
                },
                {
                    "name": "x",
                    "type": "space",
                    "unit": "micrometer"
                }
            ]
            self.assertEqual(expected_axes_metadata, attrs['multiscales'][0]['axes'])

            datasets_metadata = attrs['multiscales'][0]['datasets']
            for i, ds_metadata in enumerate(datasets_metadata):
                expected_transform = {
                    "coordinateTransformations": [
                        {
                            "scale": [
                                1.0,
                                1.0,
                                voxel_size[0] * (scale_factor ** i),
                                voxel_size[1] * (scale_factor ** i),
                                voxel_size[2] * (scale_factor ** i)
                            ],
                            "type": "scale"
                        }
                    ],
                    "path": f"{i}"
                }
                self.assertEqual(expected_transform, datasets_metadata[i])

            self.assertEqual(f"/{key}", attrs['multiscales'][0]['name'])


if __name__ == "__main__":
    unittest.main()
