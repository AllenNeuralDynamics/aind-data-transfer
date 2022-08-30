import os
from pathlib import PurePath, PurePosixPath, Path
from typing import List, Optional


def collect_filepaths(folder: str, recursive: bool = True, include_exts: Optional[List[str]] = None) -> List[str]:
    """Get the absolute paths for all files in folder
    Args:
        folder (str): the directory to look for files
        recursive (bool): whether to traverse all sub-folders
        include_exts (optional): list of valid file extensions to include.
                                 e.g., ['.tiff', '.h5', '.ims']
    Returns:
        list of filepaths
    """
    filepaths = []
    for root, _, files in os.walk(folder):
        for f in files:
            path = os.path.join(root, f)
            _, ext = os.path.splitext(path)
            if include_exts is None or ext in include_exts:
                filepaths.append(path)
        if not recursive:
            break
    return filepaths


def join_cloud_paths(cloud_dest_path: str, relpath: str) -> str:
    """Always produce posix-style paths, even if relpath
    is Windows-style
    Args:
        cloud_dest_path (str): first part of the path
        relpath (str): second part of the path
    Returns:
        the joined path
    """
    cloud_dest_path = PurePosixPath(cloud_dest_path)
    relpath = PurePath(relpath)
    return str(cloud_dest_path / relpath)


def make_cloud_paths(
    filepaths: List[str], cloud_dest_path: str, root: str = None
) -> List[str]:
    """
    Given a list of filepaths and a cloud destination folder,
    build a cloud path for each file relative to root.
    Args:
        filepaths (list): list of paths
        cloud_dest_path (str): the cloud storage path to store files
        root (str): a directory shared by all paths in filepaths, which will
                    serve as the new root under cloud_dest path. If none,
                    all files are uploaded as a flat list to cloud_dest_path,
                    ignoring any exiting directory structure.
    Returns:
        list of cloud storage paths
    Examples:
    >>> filepaths = ["/data/micr/0001.tif", "/data/metadata/rig.json"]
    >>> root = "/data"
    >>> cloud_dest_path = "my-data"
    >>> cloud_paths = make_cloud_paths(filepaths, cloud_dest_path, root)
    >>> print(cloud_paths)
    ['my-data/micr/001.tif', 'my-data/metadata/rig.json']
    >>> cloud_paths = make_cloud_paths(filepaths, cloud_dest_path, None)
    >>> print(cloud_paths)
    ['my-data/001.tif', 'my-data/rig.json']
    """
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


def is_cloud_url(url):
    if url.startswith("s3://"):
        return True
    if url.startswith("gs://"):
        return True
    return False


def parse_cloud_url(cloud_url):
    parts = Path(cloud_url).parts
    provider = parts[0] + "//"
    bucket = parts[1]
    cloud_dst = "/".join(parts[2:])
    return provider, bucket, cloud_dst
