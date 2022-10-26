To submit a job to a cluster, call `python cluster/submit.py generate-and-launch-run ...` from your login node.

Example submission:
```bash
python cluster/submit.py generate-and-launch-run \
--job_cmd="python s3_upload.py --input=my-data/ --bucket=my-bucket --s3_path=data_folder/my-data --recursive --cluster" \
--run_parent_dir="/home/user/.slurm"
--conda_activate="/path/to/conda/bin/activate" \
--conda_env="aind-data-transfer" \
--queue="aind" \
--mail_user="cameron.arshadi@alleninstitute.org" \
--ntasks_per_node=4 \
--nodes=4 \
--cpus_per_task=2 \
--mem_per_cpu=4000 \
--walltime="01:00:00"
```
This will submit the job and create a run directory named with the current timestamp in `--run_parent_dir`. 
The directory will have the following structure:
```bash
.
├── conf
│   └── jobqueue.yaml
├── logs
│   ├── dask-worker-logs
│   └── output.log
└── scripts
    └── queue-slurm-jobs.sh
```
`jobqueue.yml` contains a `dask-jobqueue` configuration generated with the parameters passed to `submit.py`. This is just a carbon
copy which is not used by Dask. The configuration is also copied to `~/.config/dask/jobqueue.yaml` prior to launch,
which is the file read by Dask.

`queue-slurm-job.sh` is the template-generated script which is submitted to the cluster manager.

To generate a run without launching it, use `python cluster/submit.py generate-run ...`

After launching a run, you can access the Dask dashboard as follows:


```bash
ssh -L 8787:<hpc-node>:8787 user.name@hpc-login
```
where `<hpc-node>` is the node the dask scheduler is running, e.g., `n253`. This node can be found in the `output.log`

Then, open a browser and navigate to `localhost:8787`
