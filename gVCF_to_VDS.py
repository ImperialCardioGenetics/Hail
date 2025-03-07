#!/usr/bin/env python
import os
import hail as hl
from hail.vds.combiner import VariantDatasetCombiner

##################################
########## INSTRUCTIONS ##########
##################################
"""
This script is designed to merge gVCF files into either a new or existing VDS and perform sample level QC. If you do not have an existing VDS, comment out the following line of code: ds_paths=[existing_vds_path].
"""
##################################
########## VARIABLES #############
##################################
"""
Define the necessary file and directory paths for the analysis. Change accordingly.
"""

# Set Resources
cpu="local[8]"
ram= '471859200'

# Path to hail logs
log = 'hail.log'

# Directory containing all gvcf files 
gvcf_files = 'pipeline/Test_gvcfs' 

# Define VDS path
output_vds = "pipeline/merge2.vds"

# Path to existing VDS (see Hail_Pipeline.py instructions)
existing_vds_path = 'pipeline/Merged.vds'

# Define combiner directory path (contains all intermediate files)
combiner_tmp = "pipeline/merge3_combiner_tmp"

# Define path for checkpoint required to restart combiner if fails
save_path = os.path.join(combiner_tmp, "combiner_plan.json")

# Set degree of file merge parralization 
BF = 100b

# Set number of gvcf per parralell job
GBS = 50

# Name and path to sample_qc table
sample_qc = 'pipeline/TEST_sample_qc.ht'

# Path to directory for MatrixTables
MT = 'pipeline/MT'

##################################
############# SCRIPT #############
##################################

hl.init(
        master=cpu,
        log=log,
        spark_conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.ui.enabled": "false",
            "spark.driver.memory": ram,
            "spark.executor.memory": ram
        }
    )
hl.default_reference='GRCh38'

def get_gvcf_files(directory):
    """
    Recursively list full paths of all gVCF files in the given directory.

    Parameters:
        directory (str): The root directory to search for gVCF files.

    Returns:
        list: A list of full paths for gVCF files.
    """
    gvcf_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.g.vcf.gz'):
                gvcf_files.append(os.path.join(root, file))
    return gvcf_files

if __name__ == '__main__':
    directory = gvcf_files
    files = get_gvcf_files(directory)
    

combiner = hl.vds.new_combiner(
        gvcf_paths=files,
        output_path=output_vds,
        temp_path=combiner_tmp,
        save_path=os.path.join(combiner_tmp, "combiner_plan.json"),
        reference_genome="GRCh38",
        use_genome_default_intervals=True,
        branch_factor=BF,
        gvcf_batch_size=GBS,
        vds_paths=[existing_vds_path] # Comment out this line if you do not have an existing VDS
    )

# Run the combiner
combiner.run()

# Load VDS
vds = hl.vds.read_vds(output_vds)

# Create sample_qc table
qc_table = hl.vds.sample_qc(
    vds,
    dp_bins=(0, 10, 20, 30, 40, 99),
    gq_bins=(0, 10, 20, 30, 40, 99),
    dp_field='DP'
)

qc_checkpoint_path = sample_qc
qc_table = qc_table.checkpoint(qc_checkpoint_path, overwrite=True)

