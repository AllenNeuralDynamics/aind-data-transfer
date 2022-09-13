"""This module contains the various api used to read data.
"""

import spikeinterface.extractors as se


class OpenEphysReader:
    """Class for readers of Open Ephys data"""

    @staticmethod
    def read(directory):
        """
        Args:
            directory (string): Input directory where data is hosted.
        """
        rec_oe = se.read_openephys(directory,
                                   {'stream_id': '0', 'block_index': 0})

        return rec_oe
