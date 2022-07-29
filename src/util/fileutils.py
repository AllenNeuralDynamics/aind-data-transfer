import os
from pathlib import PurePath, PurePosixPath
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


def join_cloud_paths(cloud_dest_path, relpath):
    """Always produce posix-style paths, even if relpath
    is Windows-style"""
    cloud_dest_path = PurePosixPath(cloud_dest_path)
    relpath = PurePath(relpath)
    return str(cloud_dest_path / relpath)


def make_cloud_paths(
    filepaths: List[str], cloud_dest_path: str, root: str = None
) -> List[str]:
    cloud_paths = []
    # remove both leading and trailing '/'
    cloud_dest_path = cloud_dest_path.strip('/')
    for fpath in filepaths:
        if root is None:
            cloud_paths.append(
                join_cloud_paths(cloud_dest_path, PurePath(fpath).name)
            )
        else:
            cloud_paths.append(
                join_cloud_paths(cloud_dest_path, os.path.relpath(fpath, root))
            )
    return cloud_paths
