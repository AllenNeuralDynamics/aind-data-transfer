# aind-data-transfer

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)

Tools for transferring large data to and between cloud storage providers.

## Installation

To upload data to aws s3, you may need to install and configure `awscli`. To upload data to gcp, you may need to install and configure `gsutil`.

### Generic upload

You may need to first install `pyminizip` from conda if getting errors on Windows: `conda install -c mzh pyminizip`

- From PyPI: `pip install aind-data-transfer`
- From source: `pip install -e .`

### Imaging

- Run `pip install -e .[imaging]`
- Run `./post_install.sh`

### Ephys

- From PyPI: `pip install aind-data-transfer[ephys]`
- From source `pip install -e .[ephys]`

### Full

- Run `pip install -e .[full]`
- Run `./post_install.sh`

#### Development

- Run `pip install -e .[dev]`
- Run `./post_install.sh`

### MPI
To run scripts on a cluster, you need to install [dask-mpi](http://mpi.dask.org/en/latest/).
This requires compiling [mpi4py](https://mpi4py.readthedocs.io/en/stable/install.html)
with the MPI implementation used by your cluster (Open MPI, MPICH, etc).
The following example is for the Allen Institute HPC, but should be applicable to other HPC systems.

SSH into your cluster login node

```ssh user.name@hpc-login```

On the Allen cluster, the MPI modules are only available on compute nodes, so SSH into a compute node (n256 chosen arbitrarily).

```ssh user.name@n256```

Now load the MPI module and compiler. It is important that you use the latest MPI version and compiler, or else 
`dask-mpi` may not function properly.

```module load gcc/10.1.0-centos7 mpi/mpich-3.2-x86_64```

Install mpi4py

```python -m pip install --no-cache-dir mpi4py```

Now install dask-mpi

```python -m pip install dask_mpi --upgrade```


## Usage

### Generic Upload Job

This job will copy the contents of a data_folder to a bucket and s3 folder with the format `modality_subject_id_date_time`. It will also attempt to create and upload metadata for the `subject_id` and register the s3 folder to a code ocean platform.

Required arguments
```
python -m aind_data_transfer.jobs.s3_upload_job --data-source "path_to_data_folder" --s3-bucket "s3_bucket" --subject-id "12345" --modality "ecephys" --acq-date "2022-12-21" --acq-time "12-00-00"
python -m aind_data_transfer.jobs.s3_upload_job -d "path_to_data_folder" -b "s3_bucket" -s "12345" -m "ecephys" -a "2022-12-21" -t "12-00-00"
```

Optional aws region (defaults to us-west-2)
```
python -m aind_data_transfer.jobs.s3_upload_job ... --s3-region "us-east-1"
python -m aind_data_transfer.jobs.s3_upload_job ... -r "us-east-1"
```

Optional service endpoints (defaults to retrieving from AWS Secrets Manager. None if not found.)
```
python -m aind_data_transfer.jobs.s3_upload_job ... --service-endpoints '{"metadata_service_url":"http://something","codeocean_domain":"https://codeocean.acme.org","codeocean_trigger_capsule":"abc-123"}'
python -m aind_data_transfer.jobs.s3_upload_job ... -e '{"metadata_service_url":"http://something","codeocean_domain":"https://codeocean.acme.org","codeocean_trigger_capsule":"abc-123"}'
```

Optional dry run (defaults to False.) If flag is set, dry-run is set to True. It will perform the operations without actually uploading or triggering the codeocean capsule. It will check that the job can hit the endpoints correctly and give a preview of the upload/trigger results:
```
python -m aind_data_transfer.jobs.s3_upload_job ... --dry-run
```

The CodeOcean API Token can be set as an env var `CODEOCEAN_API_TOKEN`. Otherwise, it will be retrieved from AWS Secrets.

### Multiple Generic Upload Jobs

You can also define the job parameters in a csv file.

```
python -m aind_data_transfer.jobs.s3_upload_job --jobs-csv-file "path_to_jobs_list"
python -m aind_data_transfer.jobs.s3_upload_job -j "path_to_jobs_list"
python -m aind_data_transfer.jobs.s3_upload_job ... --dry-run
```

The csv file should look something like:

```
data-source, s3-bucket, subject-id, modality, acq-date, acq-time
dir/data_set_1, some_bucket, 123454, ecephys, 2020-10-10, 14-10-10
dir/data_set_2, some_bucket, 123456, ecephys, 2020-10-11, 13-10-10
```

## Contributing

### Linters and testing

There are several libraries used to run linters, check documentation, and run tests.

- Please test your changes using the **coverage** library, which will run the tests and log a coverage report:

```
coverage run -m unittest discover && coverage report
```

- Use **interrogate** to check that modules, methods, etc. have been documented thoroughly:

```
interrogate .
```

- Use **flake8** to check that code is up to standards (no unused imports, etc.):

```
flake8 .
```

- Use **black** to automatically format the code into PEP standards:

```
black .
```

- Use **isort** to automatically sort import statements:

```
isort .
```

### Pull requests

For internal members, please create a branch. For external members, please fork the repo and open a pull request from the fork. We'll primarily use [Angular](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit) style for commit messages. Roughly, they should follow the pattern:
```
<type>(<scope>): <short summary>
```

where scope (optional) describes the packages affected by the code changes and type (mandatory) is one of:

- **build**: Changes that affect the build system or external dependencies (example scopes: pyproject.toml, setup.py)
- **ci**: Changes to our CI configuration files and scripts (examples: .github/workflows/ci.yml)
- **docs**: Documentation only changes
- **feat**: A new feature
- **fix**: A bug fix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **test**: Adding missing tests or correcting existing tests
