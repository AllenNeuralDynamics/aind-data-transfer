import argparse
import logging
import re

import fsspec
import zarr
from ng_link import NgState

from aind_data_transfer.util import file_utils

TILE_PATTERN = r"tile_x_\d{4}_y_\d{4}_z_\d{4}_ch_\d+"
CHANNEL_PATTERN = r"ch_(\d+)"


def _map_channel_colors(channels):
    colors = ["green", "magenta", "cyan", "yellow"]
    if len(channels) > 4:
        raise Exception("Only up to 4 channels supported.")
    color_map = {channels[i]: colors[i] for i in range(len(channels))}
    return color_map


def _parse_dimensions(zarr_path):
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


def write_json_from_zarr(
        input_zarr: str,
        output_json: str,
        vmin: float,
        vmax: float
):
    provider, bucket, cloud_path = file_utils.parse_cloud_url(input_zarr)
    protocol = provider.replace("://", "")

    fs = fsspec.filesystem(protocol)

    dimensions = _parse_dimensions(input_zarr)

    channels = []
    source_paths = []
    for f in fs.listdir(input_zarr):
        m = re.search(TILE_PATTERN, f["Key"])
        if m is None:
            continue
        tile_source = provider + f["Key"]
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
        output_json=output_json,
    )

    logging.info(f"Created link: {neuroglancer_link.get_url_link()}")
    neuroglancer_link.save_state_as_json()


def main():
    args = parse_args()
    write_json_from_zarr(args.input, args.output, args.vmin, args.vmax)


if __name__ == "__main__":
    main()
