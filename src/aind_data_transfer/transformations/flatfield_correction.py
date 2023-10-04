import logging
import os
import re
from enum import Enum

import dask
import dask.array as da
import numpy as np
import tifffile

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
        bkg_da = BkgSubtraction._adjust_bkg_dimensions(im, bkg_da)
        return da.clip(im.astype(np.int32) - bkg_da, 0, 2 ** 16 - 1).astype(np.uint16)


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
            raise ValueError(
                f"tile name does not follow convention: {tile_path}"
            )
        return os.path.join(bkg_im_dir, "bkg_" + m.group(0) + ".tiff")

    @staticmethod
    def darray_from_bkg_path(bkg_path: str, shape: tuple, chunks: tuple) -> da.Array:
        """
        Get a dask array from the background image path.

        Parameters:
            bkg_path: path to the background image.
            shape: shape of the background image.
            chunks: chunking of the background image.

        Returns:
            dask array of the background image.
        """
        bkg = dask.delayed(tifffile.imread)(bkg_path)
        bkg = da.from_delayed(bkg, shape=shape, dtype=np.uint16)
        bkg = bkg.rechunk(chunks)
        return bkg

    @staticmethod
    def _adjust_bkg_dimensions(im: da.Array, bkg_da: da.Array, pad_mode='edge') -> da.Array:
        """
        Adjust the background dimensions to match the image dimensions.

        Parameters:
            im: raw image dask Array
            bkg_data: Background data dask Array.

        Returns:
            Adjusted background data.
        """
        # Check and adjust the first dimension
        if im.shape[1] < bkg_da.shape[0]:
            _LOGGER.warning(
                f"Cropping background image's first dimension from {bkg_da.shape[0]} to {im.shape[1]}"
            )
            bkg_da = bkg_da[:im.shape[1], :]
        elif im.shape[1] > bkg_da.shape[0]:
            _LOGGER.warning(
                f"Padding background image's first dimension from {bkg_da.shape[0]} to {im.shape[1]}"
            )
            pad_width = (0, im.shape[1] - bkg_da.shape[0])
            bkg_da = da.pad(bkg_da, [(pad_width), (0, 0)], mode=pad_mode)

        # Check and adjust the second dimension
        if im.shape[2] < bkg_da.shape[1]:
            _LOGGER.warning(
                f"Cropping background image's second dimension from {bkg_da.shape[1]} to {im.shape[2]}"
            )
            bkg_da = bkg_da[:, :im.shape[2]]
        elif im.shape[2] > bkg_da.shape[1]:
            _LOGGER.warning(
                f"Padding background image's second dimension from {bkg_da.shape[1]} to {im.shape[2]}"
            )
            pad_width = (0, im.shape[2] - bkg_da.shape[1])
            bkg_da = da.pad(bkg_da, [(0, 0), (pad_width)], mode=pad_mode)

        return bkg_da
