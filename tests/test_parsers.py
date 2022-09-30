import unittest

from src.transfer.parsers import TileParser


def _create_test_data():
    channels = ["488", "512"]
    z_range = list(range(3))
    x_range = list(range(2))
    y_range = list(range(2))

    test_tiles = []
    for c in channels:
        for z in z_range:
            for y in y_range:
                for x in x_range:
                    tile = f"tile_x_{x:04}_y_{y:04}_z_{z:04}_ch_{c}.ims"
                    test_tiles.append(tile)

    expected_groups = []
    for x in x_range:
        for y in y_range:
            for c in channels:
                z_group = []
                for z in z_range:
                    z_group.append(f"tile_x_{x:04}_y_{y:04}_z_{z:04}_ch_{c}.ims")
                expected_groups.append(tuple(z_group))

    return test_tiles, expected_groups


class TestTileParser(unittest.TestCase):

    def test_group_z_tiles(self):
        paths, expected_groups = _create_test_data()
        actual_groups = TileParser.group_z_tiles(paths)
        self.assertEqual(set(expected_groups), set(actual_groups))
