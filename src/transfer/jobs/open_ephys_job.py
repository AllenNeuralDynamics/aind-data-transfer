from transfer.readers import EphysReaders
from transfer.compressors import EphysCompressors
from transfer.writers import EphysWriters

if __name__ == "__main__":
    # TODO: Convert openephys to configurable param?
    input_dir = ""
    output_dir = ""
    job_kwargs = {}
    read_blocks = EphysReaders.get_read_blocks("openephys",
                                               input_dir=input_dir)
    compressor = EphysCompressors.get_compressor("wavpack", level="3")
    scaled_read_blocks = EphysCompressors.scale_read_blocks(read_blocks)

    EphysWriters.compress_and_write_block(read_blocks=read_blocks,
                                          compressor=compressor,
                                          output_dir=output_dir,
                                          job_kwargs={})
