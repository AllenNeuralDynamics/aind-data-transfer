"""Job that reads open ephys data, compresses, and writes it."""
from transfer.compressors import EphysCompressors
from transfer.configuration_loader import EphysJobConfigurationLoader
from transfer.readers import EphysReaders
from transfer.writers import EphysWriters

if __name__ == "__main__":
    config_loader = EphysJobConfigurationLoader()
    (
        reader_conf,
        compressor_conf,
        scale_read_block_conf,
        writer_conf,
    ) = config_loader.get_configs()

    read_blocks = EphysReaders.get_read_blocks(**reader_conf)
    compressor = EphysCompressors.get_compressor(**compressor_conf)
    scaled_read_blocks = EphysCompressors.scale_read_blocks(
        read_blocks, **scale_read_block_conf
    )

    EphysWriters.compress_and_write_block(
        read_blocks=scaled_read_blocks, compressor=compressor, **writer_conf
    )
