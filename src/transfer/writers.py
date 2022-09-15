"""This module contains the api to write ephys data.
"""


class EphysWriters:
    """This class contains the methods to write ephys data."""

    @staticmethod
    def compress_and_write_block(read_blocks,
                                 compressor,
                                 output_dir,
                                 job_kwargs,
                                 output_format="zarr"):
        """
        Compress and write read_blocks.
        Args:
            read_blocks (iterable dict):
              Either [{'recording', 'block_index', 'stream_name'}] or
              [{'scale_recording', 'block_index', 'stream_name'}].
            compressor (obj): A compressor class
            output_dir (Path): Output directory to write compressed data
            job_kwargs (dict): Recording save job kwargs.
            output_format (str): Defaults to zarr

        Returns:
            Nothing. Writes data to a folder.
        """
        for read_block in read_blocks:
            rec = read_block['recording']
            block_index = read_block['block_index']
            stream_name = read_block['stream_name']
            zarr_path = output_dir / f"block{block_index}_{stream_name}.zarr"
            rec_local = rec.save(format=output_format,
                                 zarr_path=zarr_path,
                                 compressor=compressor,
                                 **job_kwargs)
