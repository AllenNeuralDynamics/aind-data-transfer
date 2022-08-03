FROM ubuntu:20.04

SHELL ["/bin/bash", "-c"] 

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN apt update --fix-missing -y && \
	apt install munge -y && \
	apt install vim -y && \
	apt install build-essential -y && \
	apt install git -y && \
	apt-get install mariadb-server -y && \
	apt install wget -y && \
	apt install -y bzip2 ca-certificates curl && \
	apt clean

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

ARG DEBIAN_FRONTEND=noninteractive

RUN apt install slurmd slurm-client -y

RUN apt install sudo -y && \
	useradd -m admin -s /usr/bin/bash -d /home/nd-data-transfer && \
	echo "admin:admin" | chpasswd && \
	adduser admin sudo && \
	echo "admin     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN source ~/.bashrc && python3 -m pip install --upgrade pip
RUN source ~/.bashrc && apt update -y && apt install libopenmpi-dev -y && python3 -m pip install mpi4py

COPY slurm.conf /etc/slurm-llnl/
COPY cgroup.conf /etc/slurm-llnl/
COPY docker-entrypoint.sh /etc/slurm-llnl/

EXPOSE 6817 6818 6819 

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
