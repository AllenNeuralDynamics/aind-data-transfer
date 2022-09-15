class EphysWriters:

    @staticmethod
    def compress_and_write_block(read_blocks,
                                 compressor,
                                 output_dir,
                                 job_kwargs,
                                 output_format="zarr"):
        for read_block in read_blocks:
            rec = read_block['recording']
            block_index = read_block['block_index']
            stream_name = read_block['stream_name']
            zarr_path = output_dir / f"block{block_index}_{stream_name}.zarr"
            rec_local = rec.save(format=output_format,
                                 zarr_path=zarr_path,
                                 compressor=compressor,
                                 **job_kwargs)
