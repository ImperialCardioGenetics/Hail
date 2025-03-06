#!/usr/bin/env python

# Set Resources
cpu =
ram =

# Path to hail logs
log = ''

# Directory containing all gvcf files 
gvcf_files = '' 

# Define VDS path
output_vds = "rds/general/user/rjanan1/home/pipeline/Test.vds"

# Path to existing VDS (see Hail_Pipeline.py instructions)
existing_vds_path = ''

# Define combiner path (contains all intermediate files)
combiner_tmp = ""

# Define path for checkpoint required to restart combiner if fails
save_path = os.path.join(combiner_tmp, "combiner_plan.json")

# Set degree of file merge parralization 
BF = 15

# Set number of gvcf per parralell job
GBS = 10

# Name and path to sample_qc table
sample_qc = ''

# Path to directory for MatrixTables
MT = 