import numpy as np
from pathlib import Path
import shutil
import spikeinterface.extractors as se

n_frames = 100

src_folder = Path("path-to-src-folder")
dst_folder = Path("path-to-dst-clip-folder")

if dst_folder.is_dir():
    shutil.rmtree(dst_folder)

# first: copy everything except .dat files
shutil.copytree(src_folder, dst_folder, ignore=shutil.ignore_patterns("*.dat"))

# use SpikeInterface to infer stream names
stream_names, stream_ids = se.get_neo_streams(
                "openephys", src_folder
            )

# clip dat files to n_frames
for dat_file in src_folder.glob("**/*.dat"):
    oe_stream_name = dat_file.parent.name
    si_stream_name = [stream_name for stream_name in stream_names if oe_stream_name in stream_name][0]
    nchan = se.read_openephys(src_folder, block_index=0,
                              stream_name=si_stream_name).get_num_channels()
    data = np.memmap(dat_file, dtype="int16",
                     order='C', mode='r').reshape(-1, nchan)
    dst_rawfile = dst_folder / str(dat_file.relative_to(src_folder))
    dst_data = np.memmap(dst_rawfile, dtype="int16",
                            shape=(n_frames, nchan), 
                            order='C', mode='w+')
    dst_data[:] = data[:n_frames]
