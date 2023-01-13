import fnmatch
import json
import os
import re
import shutil
import subprocess
from pathlib import Path, PurePath, PurePosixPath
from typing import List, Optional, Tuple, Union

from aind_data_transfer.util.io_utils import DataReaderFactory

PathLike = Union[str, Path]


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
    recursive: bool = False,
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


def create_folder(dest_dir: PathLike, verbose: Optional[bool] = False) -> None:
    """
    Create new folders.

    Parameters
    ------------------------
    dest_dir: PathLike
        Path where the folder will be created if it does not exist.
    verbose: Optional[bool]
        If we want to show information about the folder status. Default False.

    Raises
    ------------------------
    OSError:
        if the folder exists.

    """

    if not (os.path.exists(dest_dir)):
        try:
            if verbose:
                print(f"Creating new directory: {dest_dir}")
            os.makedirs(dest_dir)
        except OSError as e:
            raise


def delete_folder(dest_dir: PathLike) -> None:
    """
    Deletes a folder.

    Parameters
    ------------------------
    dest_dir: PathLike
        Path where the folder will be created if it does not exist.

    Raises
    ------------------------
    OSError:
        if the folder could not be deleted.
    """

    if os.path.exists(dest_dir):
        try:
            shutil.rmtree(dest_dir)
            print(f"Folder {dest_dir} was removed!")
        except shutil.Error as e:
            print(f"Folder could not be removed! Error {e}")


def write_list_to_txt(filename_path: PathLike, list_values: List) -> None:
    """
    Writes a list to a text file.

    Parameters
    ------------------------
    filename_path: PathLike
        Path where the file will be created.

    list_values: List
        List with the values to be written.
    """

    with open(filename_path, "w") as dicti_file:
        for value in list_values:
            dicti_file.write(f"{value}\n")


def move_folders_or_files(
    orig_path: PathLike,
    dest_path: PathLike,
    regex_folders: str,
    mode: Optional[str] = "move",
) -> None:
    """
    Move/copy folders or files to another location.

    Parameters
    ------------------------
    orig_path: PathLike
        Path where the folders/files are located.

    dest_path: PathLike
        Path where the folders/files will be located.

    regex_folders:str
        Regular expression to get folders/files

    mode:str
        Mode to move data. It could be move or copy.

    """

    orig_path = Path(orig_path)
    dest_path = Path(dest_path)

    # Convert to regular expression format
    regex_folders = "({})".format(regex_folders)

    if not os.path.isdir(orig_path) or not os.path.isdir(dest_path):
        raise ValueError("Please, check source and destination paths.")

    else:
        elements = [
            element
            for element in os.listdir(orig_path)
            if re.match(regex_folders, element)
        ]

        for element in elements:
            move_path = orig_path.joinpath(element)

            if mode == "move":
                shutil.move(str(move_path), str(dest_path))
            elif mode == "copy":

                dest_copy_path = dest_path.joinpath(element)

                if os.path.isdir(move_path):
                    shutil.copytree(str(move_path), str(dest_copy_path))

                elif os.path.isfile(move_path):
                    shutil.copyfile(str(move_path), str(dest_copy_path))

                else:
                    raise ValueError(
                        f"Element {element} is not a file nor a directory."
                    )

            else:
                raise NotImplementedError(
                    f"Mode {mode} has not been implemented."
                )


def check_path_instance(obj: object) -> bool:
    """
    Checks if an objects belongs to pathlib.Path subclasses.

    Parameters
    ------------------------
    obj: object
        Object that wants to be validated.

    Returns
    ------------------------
    bool:
        True if the object is an instance of Path subclass, False otherwise.
    """

    for childclass in Path.__subclasses__():
        if isinstance(obj, childclass):
            return True

    return False


def save_dict_as_json(
    filename: str, dictionary: dict, verbose: Optional[bool] = False
) -> None:
    """
    Saves a dictionary as a json file.

    Parameters
    ------------------------
    filename: str
        Name of the json file.
    dictionary: dict
        Dictionary that will be saved as json.
    verbose: Optional[bool]
        True if you want to print the path where the file was saved.

    """

    if dictionary == None:
        dictionary = {}

    else:
        for key, value in dictionary.items():
            # Converting path to str to dump dictionary into json
            if check_path_instance(value):
                # TODO fix the \\ encode problem in dump
                dictionary[key] = str(value)

    with open(filename, "w") as json_file:
        json.dump(dictionary, json_file, indent=4)

    if verbose:
        print(f"- Json file saved: {filename}")


def execute_command(command: str, print_command: bool = False) -> None:

    """
    Execute a shell command.

    Parameters
    ------------------------
    command: str
        Command that we want to execute.
    print_command: bool
        Bool that dictates if we print the command in the console.

    Raises
    ------------------------
    CalledProcessError:
        if the command could not be executed (Returned non-zero status).

    """

    if print_command:
        print(command)

    popen = subprocess.Popen(
        command, stdout=subprocess.PIPE, universal_newlines=True, shell=True
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield str(stdout_line).strip()
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)


def read_text_to_list(path: str):
    """
    Function to get the information saved in a file.
    Parameters:
      - path: Path where the file is located.
    Returns:
      - list
    """

    if not os.path.isfile(path):
        return False

    list_text = []

    with open(path, "r") as info_file:
        while True:
            text = info_file.readline().rstrip("\r\n")

            if not text:
                break
            else:
                list_text.append(text)

    return list_text


def get_status_filename_data(dataset_path: PathLike) -> list:
    """
    Checks the status filename data.

    Parameters
    ------------------------
    dataset_path: PathLike
        Path where the dataset will be located.

    Returns
    ------------------------
    List:
        Text file content.
    """
    STATUS_FILENAME = "DATASET_STATUS.txt"

    file_content = []

    filename_path = [
        dataset_path.joinpath(f)
        for f in os.listdir(dataset_path)
        if f == STATUS_FILENAME
        and os.path.isfile(os.path.join(dataset_path, f))
    ]
    if not len(filename_path):
        return []

    file_content = read_text_to_list(filename_path[0])
    return file_content
