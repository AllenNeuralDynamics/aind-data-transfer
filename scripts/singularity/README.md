# How to build a singularity image that can be used to run aind-data-transfer on the HPC

### Build Singularity-Builder

TODO: We should build these automatically and register them somewhere.

- From the project's root directory:
- Make a local build directory. Run `mkdir build`
- Build a docker image. Run `docker build -t singularity-builder:latest -f scripts/singularity/Dockerfile .`
- Build the singularity image. Run: `docker run --privileged -v ${PWD}/build:/build singularity-builder:latest singularity build build/aind_data_transfer.sif aind_data_transfer.def`
- There should be a sif file in the build folder. The permissions may need to be updated.
- The sif file can now be copied to a shared directory that a slurm cluster can access.
