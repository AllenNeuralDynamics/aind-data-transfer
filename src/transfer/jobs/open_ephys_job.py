import spikeinterface.extractors as se
from numcodecs import Blosc

def read_data(directory):
    return se.read_openephys(folder_path=directory,
                      stream_id='0',
                      block_index=0)

def compress_data(rec_to_compress):
    compressor = Blosc(cname="zstd", clevel=9, shuffle=Blosc.BITSHUFFLE)
    rec_to_compress.save(format="zarr", zarr_path=zarr_path,
                         compressor=compressor, **job_kwargs)