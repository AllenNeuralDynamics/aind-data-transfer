from enum import Enum

from numcodecs import Blosc


class ImagingCompressors:
    class Compressors(Enum):
        """Enum for compression algorithms a user can select"""

        blosc = Blosc.codec_id

    compressors = [member.value for member in Compressors]

    @staticmethod
    def get_compressor(compressor_name, **kwargs):
        """
        Retrieve a compressor for a given name and optional kwargs.
        Args:
            compressor_name (str): Matches one of the names Compressors enum
            **kwargs (dict): Options to pass into the Compressor
        Returns:
            An instantiated compressor class.
        """
        if compressor_name == ImagingCompressors.Compressors.blosc.name:
            return Blosc(**kwargs)
        else:
            raise Exception(
                f"Unknown compressor. Please select one of "
                f"{ImagingCompressors.compressors}"
            )
