import logging
import os
import socket
from typing import Optional, Tuple

import distributed
from dask_mpi import initialize
from distributed import Client, LocalCluster

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def log_dashboard_address(
        client: distributed.Client,
        login_node_address: str = "hpc-login"
) -> None:
    """
    Logs the terminal command required to access the Dask dashboard

    Args:
        client: the Client instance
        login_node_address: the address of the cluster login node
    """
    host = client.run_on_scheduler(socket.gethostname)
    port = client.scheduler_info()['services']['dashboard']
    user = os.getenv("USER")
    LOGGER.info(
        f"To access the dashboard, run the following in a terminal: ssh -L {port}:{host}:{port} {user}@"
        f"{login_node_address} "
    )


def get_client(
        deployment: str = "local",
        worker_options: Optional[dict] = None,
        n_workers: int = 1
) -> Tuple[distributed.Client, int]:
    """
    Create a distributed Client

    Args:
        deployment: the type of deployment. Either "local" or "slurm"
        worker_options: a dictionary of options to pass to the worker class
        n_workers: the number of workers (only applies to "local" deployment)

    Returns:
        the distributed Client and number of workers
    """
    if deployment == "slurm":
        if worker_options is None:
            worker_options = {}
        slurm_job_id = os.getenv("SLURM_JOBID")
        if slurm_job_id is None:
            raise Exception(
                "SLURM_JOBID environment variable is not set. Are you running under SLURM?"
            )
        initialize(
            nthreads=int(os.getenv("SLURM_CPUS_PER_TASK", 1)),
            local_directory=f"/scratch/fast/{slurm_job_id}",
            worker_class="distributed.nanny.Nanny",
            worker_options=worker_options
        )
        client = Client()
        log_dashboard_address(client)
        n_workers = int(os.getenv("SLURM_NTASKS"))
    elif deployment == "local":
        import platform
        use_procs = False if platform.system() == "Windows" else True
        cluster = LocalCluster(n_workers=n_workers, processes=use_procs, threads_per_worker=1)
        client = Client(cluster)
    else:
        raise NotImplementedError
    return client, n_workers
