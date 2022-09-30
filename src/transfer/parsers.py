import re
from collections import defaultdict
from pathlib import Path


class TileParserError(Exception):
    pass


class TileParser:

    @staticmethod
    def group_z_tiles(image_paths: list):
        # TODO: match more than 4 digits?
        tile_name_pattern = re.compile(r"^tile_x_\d{1,4}_y_\d{1,4}_z_\d{1,4}_ch_\d{1,4}.ims$")
        xy_pattern = re.compile(r"x_\d{1,4}_y_\d{1,4}")
        channel_pattern = re.compile(r"ch_\d{1,4}")
        z_pattern = re.compile(r"z_\d{1,4}")

        xy_tile_dict = defaultdict(list)
        grouped_tile_paths = []
        for path in image_paths:
            tile_name = Path(path).name
            m = tile_name_pattern.match(tile_name)
            if not m:
                continue
            m = xy_pattern.search(tile_name)
            xy_pos = m.group(0)
            xy_tile_dict[xy_pos].append(path)
        for tiles_at_xy in xy_tile_dict.values():
            channel_dict = defaultdict(list)
            for tile_path in tiles_at_xy:
                m = channel_pattern.search(tile_path)
                channel = m.group(0)
                channel_dict[channel].append(tile_path)
            # we want to keep Z tiles together, but channels separate
            for z_tiles in channel_dict.values():
                sorted_z_tiles = tuple(sorted(z_tiles, key=lambda x: z_pattern.search(x).group(0)))
                grouped_tile_paths.append(sorted_z_tiles)

        return grouped_tile_paths
