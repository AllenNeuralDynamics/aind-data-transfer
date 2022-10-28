import fnmatch
import os
from pathlib import PurePath, PurePosixPath, Path
from typing import List, Optional, Tuple, Union

from aind_data_transfer.util.io_utils import DataReaderFactory


def collect_filepaths(
    folder: Union[str, os.PathLike],
    recursive: bool = True,
    include_exts: Optional[List[str]] = None,
    exclude_dirs: Optional[List[str]] = None,
) -> List[str]:
    """Get the absolute paths for all files in folder
    Args:
        folder (str): the directory to look for files
        recursive (bool): whether to traverse all sub-folders
        include_exts (optional): list of valid file extensions to include.
                                 e.g., ['.tiff', '.h5', '.ims']
        exclude_dirs (optional): list of directories to exclude from the search
    Returns:
        list of filepaths
    """
    if exclude_dirs is None:
        exclude_dirs = []
    filepaths = []
    for root, _, files in os.walk(folder):
        root_name = Path(root).name
        if root_name in exclude_dirs:
            continue
        for f in files:
            path = os.path.join(root, f)
            _, ext = os.path.splitext(path)
            if include_exts is None or ext in include_exts:
                filepaths.append(path)
        if not recursive:
            break
    return filepaths


def get_images(
        image_folder: Union[str, os.PathLike],
        exclude: List[str] = None,
        include_exts: List[str] = DataReaderFactory().VALID_EXTENSIONS,
        recursive: bool = False
) -> List[str]:
    """Get the absolute paths for all images in a folder
    Args:
        image_folder: the directory to look for images
        exclude: list of filename patterns to exclude
        include_exts: list of valid file extensions to include.
                                 e.g., ['.tiff', '.h5', '.ims']
        recursive: whether to traverse all sub-folders
    Returns:
        list of image paths
    """
    if exclude is None:
        exclude = []
    image_paths = collect_filepaths(
        image_folder,
        recursive=recursive,
        include_exts=include_exts,
    )

    exclude_paths = set()
    for path in image_paths:
        if any(fnmatch.fnmatch(path, pattern) for pattern in exclude):
            exclude_paths.add(path)

    image_paths = [p for p in image_paths if p not in exclude_paths]

    return image_paths


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
    filepaths: List[Union[str, os.PathLike]],
    cloud_dest_path: Union[str, os.PathLike],
    root: Union[str, os.PathLike] = None,
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
    cloud_dest_path = cloud_dest_path.strip("/")
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


def is_cloud_url(url: str):
    """
    Test if the url points to an AWS S3 or Google Cloud Storage URI
    Args:
        url: the url to test
    Returns:
        True if url is a cloud url
    """
    url = str(url)
    if url.startswith("s3://"):
        return True
    if url.startswith("gs://"):
        return True
    return False


def parse_cloud_url(cloud_url: str) -> Tuple[str, str, str]:
    """
    Get the cloud storage provider, bucket name, and path
    from an AWS S3 or Google Cloud Storage url.
    Args:
        cloud_url: the cloud url to parse
    Returns:
        a tuple containing the provider, bucket and path
    """
    parts = Path(cloud_url).parts
    provider = parts[0] + "//"
    bucket = parts[1]
    cloud_dst = "/".join(parts[2:])
    return provider, bucket, cloud_dst
