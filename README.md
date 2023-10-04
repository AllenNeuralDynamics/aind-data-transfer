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

### Running one or more upload jobs

The jobs can be defined inside a csv file. The first row of the csv file needs the following headers. Some are required for the job to run, and others are optional.

Required

```
s3_bucket: S3 Bucket name
platform: One of [behavior, confocal, ecephys, exaSPIM, FIP, HCR, HSFP, mesoSPIM, merfish, MRI, multiplane-ophys, single-plane-ophys, SLAP2, smartSPIM] (pulled from the Platform.abbreviation field)
modality: One of [behavior-videos, confocal, ecephys, fMOST, icephys, fib, merfish, MRI, ophys, slap, SPIM, trained-behavior] (pulled from the Modality.abbreviation field)
subject_id: ID of the subject
acq_datetime: Format can be either YYYY-MM-DD HH:mm:ss or MM/DD/YYYY I:MM:SS P
```

One or more modalities need to be set. The csv headers can look like:
```
modality0: [behavior-videos, confocal, ecephys, fMOST, icephys, fib, merfish, MRI, ophys, slap, SPIM, trained-behavior]
modality0.source: path to modality0 raw data folder
modality0.compress_raw_data (Optional): Override default compression behavior. True if ECEPHYS, False otherwise.
modality0.skip_staging (Optional): If modality0.compress_raw_data is False and this is True, upload directly to s3. Default is False.
modality0.extra_configs (Optional): path to config file to override compression defaults
modality1 (Optional): [behavior-videos, confocal, ecephys, fMOST, icephys, fib, merfish, MRI, ophys, slap, SPIM, trained-behavior]
modality1.source (Optional): path to modality0 raw data folder
modality1.compress_raw_data (Optional): Override default compression behavior. True if ECEPHYS, False otherwise.
modality1.skip_staging (Optional): If modality1.compress_raw_data is False and this is True, upload directly to s3. Default is False.
modality1.extra_configs (Optional): path to config file to override compression defaults
...
```

Somewhat Optional. Set the aws_param_store_name, but can define custom endpoints if desired

```
aws_param_store_name: Path to aws_param_store_name to retrieve common endpoints
```

If aws_param_store_name not set...

```
codeocean_domain: Domain of Code Ocean platform
codeocean_trigger_capsule_id: Launch a Code Ocean pipeline
codeocean_trigger_capsule_version: Optional if Code Ocean pipeline is versioned
metadata_service_domain: Domain name of the metadata service
aind_data_transfer_repo_location: The link to this project
video_encryption_password: Password with which to encrypt video files
codeocean_api_token: Code Ocean token used to run a capsule
```

Optional

```
temp_directory: The job will use your OS's file system to create a temp directory as default. You can override the location by setting this parameter.
behavior_dir: Location where behavior data associated with the raw data is stored.
metadata_dir: Location where metadata associated with the raw data is stored.
log_level: Default log level is warning. Can be set here.
```

Optional Flags

```
metadata_dir_force: Default is false. If true, the metadata in the metadata folder will be regarded as the source of truth vs. the metadata pulled from aind_metadata_service
dry_run: Default is false. If set to true, it will perform a dry-run of the upload portion and not actually upload anything.
force_cloud_sync: Use with caution. If set to true, it will sync the local raw data to the cloud even if the cloud folder already exists.
compress_raw_data: Override all compress_raw_data defaults and set them to True.
skip_staging: For each modality, copy uncompressed data directly to s3.
```

After creating the csv file, you can run through the jobs with

```
python -m aind_data_transfer.jobs.s3_upload_job --jobs-csv-file "path_to_jobs_list"
```

Any Optional Flags attached will persist and override those set in the csv file. For example,

```
python -m aind_data_transfer.jobs.s3_upload_job --jobs-csv-file "path_to_jobs_list" --dry-run --compress-raw-data
```

will compress the raw data source and run a dry run for all jobs defined in the csv file.

An example csv file might look like:

```
data-source, s3-bucket, subject-id, modality, platform, acq-datetime, aws_param_store_name
dir/data_set_1, some_bucket, 123454, ecephys, ecephys, 2020-10-10 14:10:10, /aind/data/transfer/endpoints
dir/data_set_2, some_bucket2, 123456, ophys, multiplane-ophys, 2020-10-11 13:10:10, /aind/data/transfer/endpoints
```

### Defining a custom processing capsule to run in code ocean

Read the previous section on defining a csv file. Retrieve the capsule id from the code ocean platform.
You can add an extra parameter to define a custom processing capsule that gets executed aftet the data is uploaded:

```
codeocean_process_capsule_id, data-source, s3-bucket, subject-id, modality, platform, acq-datetime, aws_param_store_name
xyz-123-456, dir/data_set_1, some_bucket, 123454, ecephys, ecephys, 2020-10-10 14:10:10, /aind/data/transfer/endpoints
xyz-123-456, dir/data_set_2, some_bucket2, 123456, ophys, multiplane-ophys, 2020-10-11 13:10:10, /aind/data/transfer/endpoints
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
