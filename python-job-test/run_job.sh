#!/bin/bash
#SBATCH -o test.%j.out

scratch_directory="$HOME/scratch"

job_directory="${PWD}"
echo "Job directory: ${job_directory}"

# Activate appropriate conda environment
source ${HOME}/anaconda3/bin/activate single-cell

# Run the python job
python python_job_test.py --arg1="Hello there"

# Deactivate environment
conda deactivate