import os


class HDF5PluginError(Exception):
    pass


def find_hdf5plugin_path() -> str:
    """
    Finds the site-packages directory containing hdf5plugin plugins. This requires
    that hdf5plugin is installed in your current Python environment.

    Returns:
        the path to the hdf5plugin plugins directory
    """
    # this should work with both conda environments and virtualenv
    # see https://stackoverflow.com/a/46071447
    import sysconfig

    site_packages = sysconfig.get_paths()["purelib"]
    plugin_path = os.path.join(site_packages, "hdf5plugin/plugins")
    if not os.path.isdir(plugin_path):
        raise HDF5PluginError(
            f"Could not find hdf5plugin in site-packages, "
            f"{plugin_path} does not exist. "
            f"Try setting --hdf5_plugin_path manually."
        )
    return plugin_path
