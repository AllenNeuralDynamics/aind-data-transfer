FROM ubuntu:22.04

# Install packages

RUN apt-get update && apt-get install -y \
build-essential \
libssl-dev \
uuid-dev \
libgpgme11-dev \
squashfs-tools \
libseccomp-dev \
wget \
pkg-config \
procps

# Download Go source, install them and modify the PATH

ARG GO_VERSION=1.14.12 
ARG GO_OS=linux 
ARG GO_ARCH=amd64

RUN wget https://dl.google.com/go/go$GO_VERSION.$GO_OS-$GO_ARCH.tar.gz && \
tar -C /usr/local -xzvf go$GO_VERSION.$GO_OS-$GO_ARCH.tar.gz && \
rm go$GO_VERSION.$GO_OS-$GO_ARCH.tar.gz && \
echo 'export PATH=$PATH:/usr/local/go/bin' | tee -a /etc/profile

# Download Singularity


ARG SINGULARITY_VERSION=3.7.0

RUN wget https://github.com/hpcng/singularity/releases/download/v${SINGULARITY_VERSION}/singularity-${SINGULARITY_VERSION}.tar.gz && \
tar -xzf singularity-${SINGULARITY_VERSION}.tar.gz &&\
rm singularity-${SINGULARITY_VERSION}.tar.gz

# Compile Singularity sources and install it

RUN export PATH=$PATH:/usr/local/go/bin && \
cd singularity && \
./mconfig --without-suid && \
make -C ./builddir && \
make -C ./builddir install

# Create a directory to store the sif file

RUN mkdir build

# Copy def file and post_install script
COPY ["scripts/singularity/aind_data_transfer.def", "scripts/singularity/build_sif.sh","post_install.sh", "./"]
