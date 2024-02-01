import logging
import os
import socket
from enum import Enum
from typing import Optional, Tuple

import requests
import distributed
from distributed import Client, LocalCluster

try:
    from dask_mpi import initialize
    DASK_MPI_INSTALLED = True
except ImportError:
    DASK_MPI_INSTALLED = False

LOGGER = logging.getLogger(__name__)


class Deployment(Enum):
    LOCAL = "local"
    SLURM = "slurm"


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


def get_deployment() -> str:
    if os.getenv("SLURM_JOBID") is None:
        deployment = Deployment.LOCAL.value
    else:
        # we're running on the Allen HPC
        deployment = Deployment.SLURM.value
    return deployment


def get_client(
        deployment: str = Deployment.LOCAL.value,
        worker_options: Optional[dict] = None,
        n_workers: int = 1,
        processes=True
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
    if deployment == Deployment.SLURM.value:
        if not DASK_MPI_INSTALLED:
            raise ImportError(
                "dask-mpi must be installed to use the SLURM deployment"
            )
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
    elif deployment == Deployment.LOCAL.value:
        client = Client(LocalCluster(n_workers=n_workers, processes=processes, threads_per_worker=1))
    else:
        raise NotImplementedError
    return client, n_workers


def cancel_slurm_job(job_id: str, api_url: str, headers: dict) -> requests.Response:
    """
    Attempt to release resources and cancel the job

    Args:
        job_id: the SLURM job ID
        api_url: the URL of the SLURM REST API. E.g., "http://myhost:80/api/slurm/v0.0.36"

    Raises:
        HTTPError: if the request to cancel the job fails
    """
    # Attempt to release resources and cancel the job
    # Workaround for https://github.com/dask/dask-mpi/issues/87
    endpoint = f"{api_url}/job/{job_id}"
    response = requests.delete(endpoint, headers=headers)

    return response

