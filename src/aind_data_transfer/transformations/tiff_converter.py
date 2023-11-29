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

from aind_data_transfer.utils import get_logger

class BaseConverter:
    def __init__(self, input_dir: Path, output_dir: Path, unique_id: str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.unique_id = unique_id

    @property
    def _read_and_sort_images(self) -> List[Path]:
        """
        Read in all the images in a directory and sort them by creation time

        Parameters
        ----------
        input_dir : Path
            The directory containing the images

        Returns
        -------
        Enumeration
            A sorted list of paths to the images sorted by creation time
        """
        # Sort images by creation time
        sorted_tiff_images = sorted(list(self.input_dir.glob("*.tif")), key=os.path.getctime)
        return enumerate(sorted_tiff_images)

    def _cache_images(
        self, image_data_to_cache: List[np.ndarray], initial_frame: int, cache_filepath: Path | str
    ) -> None:
        """
        Cache images to disk

        Parameters
        ----------
        image_data_to_cache : List[np.ndarray]
            A list of image data to cache
        initial_frame : int
            The index of the first frame to concatenate to the array
        cache_filepath : Path | str
            The filepath to the cache file (should be a temporary file)

        Returns
        -------
        """
        with h5.File(cache_filepath, "a") as f:
            f["data"].resize(initial_frame + len(image_data_to_cache), axis=0)
            f["data"][
                initial_frame : initial_frame + len(image_data_to_cache)
            ] = image_data_to_cache
        # import pdb;pdb.set_trace()

    def _write_final_output(
        self,
        output_filepath: Path | str,
        tmp_filepath: Path | str,
        img_width=512,
        img_height=512,
        chunk_size=100,
        **kwargs: dict,
    ):
        """Writes the final output to disk along with the metadata. Clears the temporary hdf5 data file

        Parameters
        ----------
        output_filepath : Path | str
            The filepath to the output file
        tmp_fileath: Path_str
            The filepath to the temporary file
        image_width : int, optional
            The width of the image, by default 512
        image_height : int, optional
            The height of the image, by default 512
        chunk_size : int, optional
            The chunk size to write to disk, by default 100
        **kwargs : dict
            The metadata to write to disk

        Returns
        -------
        None
        """
        with h5.File(output_filepath, "w") as f:
            for key, value in kwargs.items():
                f.create_dataset(key, data=value)
            f.create_dataset(
                "data",
                (chunk_size, img_width, img_height),
                chunks=True,
                maxshape=(None, img_width, img_height),
            )
            with h5.File(tmp_filepath, "r") as tmp_f:
                chunked_time = dt.now()
                data_length = tmp_f["data"].shape[0]
                logging.info(f"Data length {data_length}")
                for i in range(0, data_length, chunk_size):
                    if i + chunk_size < data_length:
                        f["data"].resize(i + chunk_size, axis=0)
                        f["data"][i : i + chunk_size] = tmp_f["data"][i : i + chunk_size]
                    else:
                        f["data"].resize(data_length, axis=0)
                        f["data"][i:] = tmp_f["data"][i:]
                chunked_end_time = (dt.now() - chunked_time).seconds
                logging.info(f"Chunked write took {chunked_end_time} seconds")


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
        sorted_tiff_images = self._read_and_sort_images
        # Find all the unique stages acquired
        tiff_trial_groups = set(
            [
                "_".join(image_path.name.split("_")[:-1])
                for image_path in self.input_dir.glob("*.tif")
            ]
        )
        # tiff_data = {key:list(self.input_dir.glob(f"{key}*.tif")) for key in tiff_set}
        # tiff_data = {key:sorted(value, key=os.path.getctime) for (key, value) in tiff_data.items()}
        return tiff_trial_groups, sorted_tiff_images

    def cache_and_convert_images(
        self,
        tiff_images: List,
        trials: set,
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
        trials : set
            A set of unique trial names
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
        # metadata dictionary that keeps track of the trial name and the location of the 
        # trial image in the stack
        trial_slice_location = {trial: [] for trial in trials}
        # metadata dictionary where the keys are the image filename and the 
        # values are the index of the order in which the image was read, which 
        # trial it's associated with, and the location of the image in the stack and the 
        # image shape
        lookup_table = {str(filename): {"index": index} for index, filename in tiff_images}
        tmp_file = TemporaryFile(suffix=".h5")
        with h5.File(tmp_file, "w") as f:
            f.create_dataset(
                "data",
                (0, image_width, image_height),
                chunks=True,
                maxshape=(None, image_width, image_height),
            )
        for filename in lookup_table.keys():
            logging.info(f"Processing {os.path.basename(filename)}...")
            image_shape = ScanImageTiffReader(str(filename)).shape()
            logging.info(f"Image shape {image_shape}")
            image_data = ScanImageTiffReader(str(filename)).data()
            if image_shape[0] + images_stored >= cache_size:
                frames_to_store = cache_size - images_stored
                image_buffer[images_stored:cache_size] = image_data[:frames_to_store]
                total_count += frames_to_store
                self._cache_images(image_buffer, total_count - cache_size, tmp_file)
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
            lookup_table[filename]["trial"] = "_".join(os.path.basename(filename).split("_")[:-1])
            lookup_table[filename]["location_in_stack"] = [
                total_count,
                total_count + frames_to_store,
            ]
            trial_slice_location["_".join(os.path.basename(filename).split("_")[:-1])].append(
                (total_count, total_count + image_shape[0])
            )
        # if images did not get cached to disk, cache them now
        if images_stored > 0:
            final_buffer = np.zeros((images_stored, image_width, image_height))
            final_buffer[:images_stored] = image_buffer[:images_stored]
            self._cache_images(final_buffer, total_count - images_stored, tmp_file)
        logging.info(f"Total count {total_count}")
        self._write_final_output(
            output_filepath,
            tmp_file,
            img_width=image_width,
            img_height=image_height,
            chunk_size=cache_size,
            trial_slice_location=json.dumps(trial_slice_location),
            lookup_table=json.dumps(lookup_table),
        )
        os.remove(tmp_file.name)
        total_time = dt.now() - start_time
        logging.info(f"Total time to convert {total_time.seconds} seconds")
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
        tiff_trial_groups, sorted_image_list = self._build_tiff_data_structure()
        output_filepath = self.cache_and_convert_images(
            sorted_image_list,
            cache_size=chunk_size,
            trials=tiff_trial_groups,
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
