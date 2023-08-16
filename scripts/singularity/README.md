# How to build a singularity image that can be used to run aind-data-transfer on the HPC

### Build Singularity-Builder

TODO: We should build the singularity-builder docker container in a different repo

- From the project's root directory and from the main branch:
- Run: `./scripts/singularity/run_build.sh`
- After the build completes, there should be a sif file that can now be copied to a shared directory that a slurm cluster can access.

### To build an older version other than the latest
- If the docker image is already built, you can create a sif file for a specific version by running: `docker run --privileged -e AIND_DATA_TRANSFER_VERSION=${AIND_DATA_TRANSFER_VERSION} -v ${PWD}/build:/build singularity-builder:latest ./build_sif.sh`
- Alternatively, you can checkout a specific tag and run `./scripts/singularity/run_build.sh`
