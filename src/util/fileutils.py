import os
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