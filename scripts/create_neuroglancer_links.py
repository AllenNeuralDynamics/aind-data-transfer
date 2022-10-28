import re

import s3fs
from ng_link import NgState


def map_channel_colors(channels):
    colors = ['green', 'red', 'blue', 'yellow']
    if len(channels) > 4:
        raise Exception("Only up to 4 channels supported.")
    color_map = {channels[i]: colors[i] for i in range(len(channels))}
    return color_map


def main():
    zarr_path = r"s3://aind-open-data/exaSPIM_614978_2022-10-21_12-27-00/exaSPIM/"

    tile_pattern = r"tile_x_\d{4}_y_\d{4}_z_\d{4}_ch_\d+"
    channel_pattern = r"ch_(\d+)"

    dimensions = {
        # check the order
        "t": {
            "voxel_size": 0.001,
            "unit": 'seconds'
        },
        'c': {
            "voxel_size": 1.0,
            "unit": "microns"
        },
        "z": {
            "voxel_size": 1.0,
            "unit": 'microns'
        },
        "y": {
            "voxel_size": 0.75,
            "unit": 'microns'
        },
        "x": {
            "voxel_size": 0.75,
            "unit": 'microns'
        },
    }

    fs = s3fs.S3FileSystem()
    channels = []
    source_paths = []
    for f in fs.listdir(zarr_path):
        m = re.search(tile_pattern, f['Key'])
        if m is None:
            continue
        tile_source = "zarr://s3://" + f['Key']
        print(tile_source)
        source_paths.append(tile_source)
        tile_name = m.group(0)
        m = re.search(channel_pattern, tile_name)
        channel = int(m.group(1))
        channels.append(channel)

    unique_channels = set(channels)
    color_map = map_channel_colors(list(unique_channels))

    layers = []
    for source, ch in zip(source_paths, channels):
        layer = {
            'source': source,
            # 'channel': channels.index(ch),
            "tab": "rendering",
            "blend": "additive",
            'shader': {
                'color': color_map[ch],
                'emitter': 'RGB',
                'vec': 'vec3'
            },
            'shaderControls': {  # Optional
                "normalized": {
                    "range": [30, 50]
                }
            },
        }
        layers.append(layer)

    state = {
        "dimensions": dimensions,
        "layers": layers
    }

    neuroglancer_link = NgState(
        input_config=state,
        mount_service='s3',
        bucket_path='aind-open-data/exaSPIM_614978_2022-10-21_12-27-00',
        output_json="s3://aind-open-data/exaSPIM_614978_2022-10-21_12-27-00/ng-link.json",
    )

    neuroglancer_link.save_state_as_json()
    #print(neuroglancer_link.get_url_link())

if __name__ == "__main__":
    main()