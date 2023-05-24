import logging
import os
import re
from enum import Enum

import dask.array as da

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


class BkgSubtraction:

    class Regexes(Enum):
        tile_xyz_pattern = r"tile_[xX]_\d{4}_[yY]_\d{4}_[zZ]_\d{4}"

    @staticmethod
    def subtract(im: da.Array, bkg_da: da.Array) -> da.Array:
        """
        Subtract the background image from the input image volume.

        Parameters:
            im: 3D raw image volume dask array.
            bkg_im: 2D background image dask array.

        Returns:
            Resulting image volume after background subtraction.
        """
        bkg_da = da.pad(
            bkg_da,
            [(0, im.shape[1] - bkg_da.shape[0]), (0, im.shape[2] - bkg_da.shape[1])],
            mode="edge"
        )
        return da.clip((im + 100) - bkg_da, 100, 2 ** 16 - 1) - 100

    @staticmethod
    def get_bkg_path(tile_path: str, bkg_im_dir: str) -> str:
        """
        Get the path to the corresponding background image from the raw tile path and background image directory.

        Parameters:
            tile_path: path to the raw image volume.
            bkg_im_dir: path to the background image directory.

        Returns:
            the path to the background image corresponding to the raw image.
        """
        m = re.search(BkgSubtraction.Regexes.tile_xyz_pattern.value, tile_path)
        if m is None:
            raise ValueError(f"tile name does not follow convention: {tile_path}")
        return os.path.join(bkg_im_dir, "bkg_" + m.group(0) + ".tiff")






