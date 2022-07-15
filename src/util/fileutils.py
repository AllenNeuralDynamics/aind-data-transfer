import os
from pathlib import PurePath
from typing import List


def collect_filepaths(folder: str, recursive: bool = True) -> List[str]:
    """Get the absolute paths for all files in folder
    Args:
        folder (str): the directory to look for files
        recursive (bool): whether to traverse all sub-folders
    Returns:
        list of filepaths
    """
    filepaths = []
    for root, _, files in os.walk(folder):
        for f in files:
            filepaths.append(os.path.join(root, f))
        if not recursive:
            break
    return filepaths


def make_cloud_paths(
    filepaths: List[str], cloud_dest_path: str, root: str = None
) -> List[str]:
    cloud_paths = []
    for fpath in filepaths:
        if root is None:
            cloud_paths.append(
                os.path.join(cloud_dest_path, PurePath(fpath).name)
            )
        else:
            cloud_paths.append(
                os.path.join(cloud_dest_path, os.path.relpath(fpath, root))
            )
    return cloud_paths
