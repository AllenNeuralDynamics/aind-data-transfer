import logging
from ScanImageTiffReader import ScanImageTiffReader
from pathlib import Path
import os
import h5py as h5
from typing import List
import numpy as np
import json
from datetime import datetime as dt
from tempfile import TemporaryFile
import re

class BaseConverter:
    def __init__(self, input_dir: Path, output_dir: Path, unique_id: str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.unique_id = unique_id

    def _read_and_sort_images(self, exclusion_pattern: str = None) -> List[Path]:
        """
        Read in all the images in a directory and sort them by creation time

        Parameters
        ----------
        input_dir : Path
            The directory containing the images
        exclusion_pattern : str
            patterns to be excluded from the file search
        Returns
        -------
        Enumeration
            A sorted list of paths to the images sorted by creation time
        """
        # Sort images by creation time
        sorted_tiff_images = sorted(list(self.input_dir.glob("*.tif")), key=os.path.getctime)
        if exclusion_pattern:
            exclude_pattern = re.compile(r"{}".format(exclusion_pattern))
            sorted_tiff_images = [image for image in sorted_tiff_images if not exclude_pattern.search(str(image))]
        return enumerate(sorted_tiff_images)

    def _cache_images(
        self, image_data_to_cache: List[np.ndarray], initial_frame: int, cache_filepath: Path
    ) -> None:
        """
        Cache images to disk

        Parameters
        ----------
        image_data_to_cache : List[np.ndarray]
            A list of image data to cache
        initial_frame : int
            The index of the first frame to concatenate to the array
        cache_filepath : Path
            The filepath to the cache file (should be a temporary file)

        Returns
        -------
        """
        with h5.File(cache_filepath, "a") as f:
            f["data"].resize(initial_frame + len(image_data_to_cache), axis=0)
            f["data"][
                initial_frame : initial_frame + len(image_data_to_cache)
            ] = image_data_to_cache

    def _write_final_output(
        self,
        output_filepath: Path,
        **kwargs: dict,
    ):
        """Writes the final output to disk along with the metadata. Clears the temporary hdf5 data file

        Parameters
        ----------
        output_filepath : Path
            The filepath to the output file
        **kwargs : dict
            The metadata to write to disk

        Returns
        -------
        None
        """

        # b/c this seems to take a long time
        start_time = dt.now()
        with h5.File(output_filepath, "a") as f:
            for key, value in kwargs.items():
                meta_size = len(value)
                f.create_dataset(key, (meta_size), maxshape=(meta_size,), dtype=h5.special_dtype(vlen=str))
                f[key].resize(meta_size, axis=0)
                f[key][:] = value 
        total_time = (dt.now() - start_time).seconds
        print(f"Time to add the metadata to h5 {total_time} seconds")


class BergamoConverter(BaseConverter):
    def __init__(self, input_dir: Path, output_dir: Path, unique_id: str):
        super().__init__(input_dir, output_dir, unique_id)

    def _build_tiff_data_structure(self) -> dict:
        """Builds tiff data structures used for the header data later

        Returns
        -------
        dict
            A dictionary containing the tiff data structure
        list
            Index to filepath mapping
        """

        ## Associate an index with each image
        sorted_tiff_images = self._read_and_sort_images(exclusion_pattern = "stack")
        # Find all the unique stages acquired
        tiff_epochs = set(
            [
                "_".join(image_path.name.split("_")[:-1])
                for image_path in self.input_dir.glob("*.tif")
            ]
        )
        return tiff_epochs, sorted_tiff_images

    def cache_and_convert_images(
        self,
        tiff_images: List,
        epochs: set,
        cache_size=100,
        image_width: int = 512,
        image_height: int = 512,
    ) -> Path:
        """
        Reads in a list of tiff files from a specified path (initialized above) and converts them
        to a single h5 file

        Parameters
        ----------
        tiff_images : Enumeration
            A sorted list of paths to the images sorted by creation time
        epochs : set
            A set of unique epoch names
        cache_size : int, optional
            The number of images to cache in memory, by default 100
        image_width : int, optional
            The width of the image, by default 512
        image_height : int, optional
            The height of the image, by default 512

        Returns
        -------
        Path
            converted filepath
        dict
            key is the image name, value is the image shape
        """
        start_time = dt.now()
        # to keep track of all the images stored to disk
        total_count = 0
        # to keep track of the number of images stored in memory
        images_stored = 0
        # to keep track of the number of images to store to disk when there is buffer overflow
        frames_to_store = 0
        image_buffer = np.zeros((cache_size, image_width, image_height))
        output_filepath = self.output_dir / f"{self.unique_id}.h5"
        start_epoch_count = 0
        previous_epoch_name = None
        # metadata dictionary that keeps track of the epoch name and the location of the 
        # epoch image in the stack
        epoch_slice_location = {epoch: [] for epoch in epochs}
        # metadata dictionary where the keys are the image filename and the 
        # values are the index of the order in which the image was read, which 
        # epoch it's associated with,  the location of the image in the h5 stack and the 
        # image shape
        lookup_table = {str(filename): {"index": index} for index, filename in tiff_images}
        # tmp_file = TemporaryFile(suffix=".h5")
        with h5.File(output_filepath, "w") as f:
            f.create_dataset(
                "data",
                (0, image_width, image_height),
                chunks=True,
                maxshape=(None, image_width, image_height),
            )
        for filename in lookup_table.keys():
            # Grabbing the epoch name to keep track of changes and 
            # index position of each epoch in the stack. Will compare to previous_epoch_name
            epoch_name = "_".join(os.path.basename(filename).split("_")[:-1])
            print(f"Processing {os.path.basename(filename)}...")
            image_shape = ScanImageTiffReader(str(filename)).shape()
            print(f"Image shape {image_shape}")
            image_data = ScanImageTiffReader(str(filename)).data()
            if image_shape[0] + images_stored >= cache_size:
                frames_to_store = cache_size - images_stored
                image_buffer[images_stored:cache_size] = image_data[:frames_to_store]
                total_count += frames_to_store
                self._cache_images(image_buffer, total_count - cache_size, output_filepath)
                images_stored = 0
                image_buffer = np.zeros((cache_size, image_width, image_height))
                image_buffer[images_stored : image_shape[0] - frames_to_store] = image_data[
                    frames_to_store:
                ]
                images_stored += image_shape[0] - frames_to_store
                total_count += image_shape[0] - frames_to_store
            else:
                image_buffer[images_stored : images_stored + image_shape[0]] = image_data
                images_stored += image_shape[0]
                total_count += image_shape[0]

            lookup_table[filename]["image_shape"] = image_shape
            lookup_table[filename]["epoch"] = epoch_name
            lookup_table[filename]["location_in_stack"] = [
                total_count,
                total_count + frames_to_store,
            ]
            if previous_epoch_name is None and start_epoch_count == 0:
                previous_epoch_name = epoch_name
            elif epoch_name != previous_epoch_name:
                epoch_slice_location[previous_epoch_name].append(
                    (start_epoch_count, total_count)
                )
                epoch_name = lookup_table[filename]["epoch"]
                start_epoch_count = total_count + 1
                previous_epoch_name = epoch_name
        # save the last epoch slice location
        epoch_slice_location[epoch_name].append(
                (start_epoch_count, total_count)
            )
        # if images did not get cached to disk, cache them now
        if images_stored > 0:
            final_buffer = np.zeros((images_stored, image_width, image_height))
            final_buffer[:images_stored] = image_buffer[:images_stored]
            self._cache_images(final_buffer, total_count - images_stored, output_filepath)
        print(f"Total count {total_count}")
        self._write_final_output(
            output_filepath,
            epoch_slice_location=json.dumps(epoch_slice_location),
            lookup_table=json.dumps(lookup_table),
        )
        total_time = dt.now() - start_time
        print(f"Time to cache {total_time.seconds} seconds")
        return self.output_dir / f"{self.unique_id}.h5"

    def write_bergamo_to_h5(self, chunk_size=500) -> Path:
        """
        Reads in a list of tiff files from a specified path (initialized above) and converts them
        to a single h5 file. Writes relevant metadata to the h5 file.

        Parameters
        ----------
        chunk_size : int, optional
            The chunk size to write to disk, by default 500
        Returns
        -------
        Path
            converted filepath
        """

        # Convert the file and build the final metadata structure
        tiff_epochs, sorted_image_list = self._build_tiff_data_structure()
        output_filepath = self.cache_and_convert_images(
            sorted_image_list,
            cache_size=chunk_size,
            epochs=tiff_epochs,
            image_width=800,
            image_height=800,
        )
        return output_filepath

class MesoscopeConverter(BaseConverter):
    pass


class FileSplitter:
    pass


if __name__ == "__main__":
    input_dir = Path(r"D:\bergamo\data")
    output_dir = Path(r"D:")
    unique_id = "bergamo"
    bergamo_converter = BergamoConverter(input_dir, output_dir, unique_id)
    bergamo_converter.write_bergamo_to_h5()
