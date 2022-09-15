"""Job that reads open ephys data, compresses, and writes it."""
from transfer.readers import EphysReaders
from transfer.compressors import EphysCompressors
from transfer.writers import EphysWriters
from transfer.configuration_loader import EphysJobConfigs

if __name__ == "__main__":
    reader_configs = EphysJobConfigs.read_configs
    writer_configs = EphysJobConfigs.write_configs
    compressor_configs = EphysJobConfigs.compressor_configs

    read_blocks = EphysReaders.get_read_blocks(**reader_configs)
    compressor = EphysCompressors.get_compressor(**compressor_configs)
    scaled_read_blocks = EphysCompressors.scale_read_blocks(read_blocks)

    EphysWriters.compress_and_write_block(read_blocks=read_blocks,
                                          compressor=compressor,
                                          **writer_configs)
