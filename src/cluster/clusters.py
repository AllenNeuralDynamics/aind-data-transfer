import os
from pathlib import Path

from dask_jobqueue import SLURMCluster
import logging

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_slurm_cluster(
    n_workers: int,
    cores: int,
    processes: int,
    memory: str,
    queue: str,
    walltime: str,
    death_timeout: str = "600s",
    **kwargs,
) -> SLURMCluster:
    home_folder = os.environ["HOME"]
    logger.info(kwargs)
    if "log_directory" not in kwargs:
        print("not in kwargs")
        log_dir = f"{home_folder}/.dask_distributed/"
        Path(log_dir).mkdir(parents=False, exist_ok=True)
        kwargs["log_directory"] = log_dir

    if "local_directory" not in kwargs:
        local_dir = f"{home_folder}/.dask_worker"
        Path(local_dir).mkdir(parents=False, exist_ok=True)
        kwargs["local_directory"] = local_dir

    cluster = SLURMCluster(
        queue=queue,
        cores=cores,
        processes=processes,
        memory=memory,
        walltime=walltime,
        death_timeout=death_timeout,
        **kwargs,
    )
    cluster.scale(n_workers)
    return cluster
