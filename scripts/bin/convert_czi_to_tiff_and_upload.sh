#!/bin/bash

#SBATCH --cpus-per-task=20
#SBATCH --mem-per-cpu=8000
#SBATCH --tmp=128MB
#SBATCH --time=4:00:00
#SBATCH --partition=aind
#SBATCH --output=/allen/aind/scratch/carson.berry/hpc_outputs/%j_czi_conversion.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=carson.berry@alleninstitute.org
#SBATCH --ntasks=1

source "/allen/programs/mindscope/workgroups/omfish/carsonb/miniconda/bin/activate" adt-upload-clone
czi_loc="/allen/aind/stage/Z1/Jazmin/photobleaching_pilot/after_treatment/Overnight"
tiff_loc="/allen/aind/stage/Z1/HCR_718733-pt-overnight-test_2024-05-06_09-00-00"
metadata_only="False"

#make directories
mkdir -p $tiff_loc/diSPIM
mkdir -p $tiff_loc/derivatives
tiff_loc=$tiff_loc/diSPIM

python /allen/programs/mindscope/workgroups/omfish/carsonb/aind-data-transfer/scripts/ispim_cron_job.py --input_folder $czi_loc --output_folder $tiff_loc --metadata_only $metadata_only

#remove the czi files if you don't need 'em
# rm $czi_loc/*.czi

