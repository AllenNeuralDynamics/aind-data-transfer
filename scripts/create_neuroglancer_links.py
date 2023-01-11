import argparse
import logging
import re

import fsspec
import zarr
from fsspec import AbstractFileSystem
from gcsfs import GCSFileSystem
from ng_link import NgState
from s3fs import S3FileSystem

from aind_data_transfer.util import file_utils

TILE_PATTERN = r"tile_x_\d{4}_y_\d{4}_z_\d{4}_ch_\d+"
CHANNEL_PATTERN = r"ch_(\d+)"


def _map_channel_colors(channels: list) -> dict:
    """
    Map a list of channel names to unique colors. Only up to 4 channels are
    currently supported.

    Args:
        channels: a list of channel names

    Returns:
        a dict mapping channel names to colors
    """
    colors = ["green", "magenta", "cyan", "yellow"]
    if len(channels) > 4:
        raise Exception("Only up to 4 channels supported.")
    color_map = {channels[i]: colors[i] for i in range(len(channels))}
    return color_map


def _parse_dimensions(zarr_path: str) -> dict:
    """
    Given a url to a zarr dataset, create a dictionary of the dimensions
    used to construct the neuroglancer state. Each dimension has a
    voxel_size field and a unit field, except for channel (c'),
    which has an empty string for unit.

    Args:
        zarr_path: the url of the zarr dataset

    Returns:
        a dict of dimensions
    """
    z = zarr.open(zarr_path, "r")
    attrs = dict(z[next(iter(z.keys()))].attrs)
    ms = attrs["multiscales"][0]
    axes = {el["name"]: el for el in ms["axes"]}
    full_res = [ds for ds in ms["datasets"] if ds["path"] == "0"][0]
    scale = [
        ct
        for ct in full_res["coordinateTransformations"]
        if ct["type"] == "scale"
    ][0]
    voxel_size = scale["scale"]
    dimensions = {
        "x": {"voxel_size": voxel_size[4], "unit": axes["x"]["unit"]},
        "y": {"voxel_size": voxel_size[3], "unit": axes["y"]["unit"]},
        "z": {"voxel_size": voxel_size[2], "unit": axes["z"]["unit"]},
        "c'": {"voxel_size": voxel_size[1], "unit": ""},
        "t": {"voxel_size": voxel_size[0], "unit": axes["t"]["unit"]},
    }
    return dimensions


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="Path to the dataset")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="directory to output the process_output.json file",
    )
    parser.add_argument(
        "--vmin", type=float, default=0, help="display range minimum"
    )
    parser.add_argument(
        "--vmax", type=float, default=500.0, help="display range maximum"
    )
    return parser.parse_args()


def _get_object_path_key(fs: AbstractFileSystem) -> str:
    """
    Given a fsspec AbstractFileSystem, return the key used to access the
    object path

    Args:
        fs: the AbstractFileSystem instance

    Returns
        the key used to access an object path
    """
    if isinstance(fs, S3FileSystem):
        return "Key"
    elif isinstance(fs, GCSFileSystem):
        return "name"
    else:
        raise NotImplementedError


def write_json_from_zarr(
    input_zarr: str, out_json_dir: str, vmin: float, vmax: float
) -> None:
    """
    Create a "process_output.json" file with a neuroglancer link to the
    zarr data. The link is found in the "ng_link" field.

    Args:
        input_zarr: the url of the zarr dataset
        out_json_dir: the directory to write the "process_output.json" file
        vmin: the default minimum of the neuroglancer display range
        vmax: the default maximum of the neuroglancer display range
    """
    provider, bucket, cloud_path = file_utils.parse_cloud_url(input_zarr)
    protocol = provider.replace("://", "")

    fs = fsspec.filesystem(protocol)

    dimensions = _parse_dimensions(input_zarr)

    channels = []
    source_paths = []
    for f in fs.listdir(input_zarr):
        tile_path_field = _get_object_path_key(fs)
        m = re.search(TILE_PATTERN, f[tile_path_field])
        if m is None:
            continue
        tile_source = provider + f[tile_path_field]
        source_paths.append(tile_source)
        tile_name = m.group(0)
        m = re.search(CHANNEL_PATTERN, tile_name)
        channel = int(m.group(1))
        channels.append(channel)

    unique_channels = set(channels)
    color_map = _map_channel_colors(list(unique_channels))

    layers = []
    for source, ch in zip(source_paths, channels):
        layer = {
            "source": source,
            "tab": "rendering",
            "blend": "additive",
            "shader": {
                "color": color_map[ch],
                "emitter": "RGB",
                "vec": "vec3",
            },
            "shaderControls": {  # Optional
                "normalized": {"range": [vmin, vmax]}
            },
        }
        layers.append(layer)

    state = {"dimensions": dimensions, "layers": layers}

    neuroglancer_link = NgState(
        input_config=state,
        mount_service=protocol,
        bucket_path=bucket,
        output_json=out_json_dir,
    )

    logging.info(f"Created link: {neuroglancer_link.get_url_link()}")
    neuroglancer_link.save_state_as_json()


def main():
    args = parse_args()
    write_json_from_zarr(args.input, args.output, args.vmin, args.vmax)


if __name__ == "__main__":
    main()
