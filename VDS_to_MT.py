#!/usr/bin/env python
import os
import hail as hl
from hail.vds.combiner import VariantDatasetCombiner

##################################
########## INSTRUCTIONS ##########
##################################
"""
This script is designed to partition VDS into MatrixTables by Chr with variant level QC using Hail. 
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

# Path to VDS
vds = 'pipeline/merge2.vds'

# Directory to output MatrixTables. Change this variable to your desired output directory.
output_dir = 'pipeline/MT'


##################################
############# SCRIPT #############
##################################

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

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
hl.default_reference = 'GRCh38'

vds = hl.vds.read_vds(vds)

# List of chromosomes to process
chromosomes = [f'chr{i}' for i in range(1, 23)] + ['chrX', 'chrY', 'chrM']

for chrom in chromosomes:
    # Define the interval for the current chromosome
    interval = hl.parse_locus_interval(chrom, reference_genome='GRCh38')
    
    # Filter VDS to the current chromosome
    vds_chr = hl.vds.filter_intervals(vds, [interval], keep=True)
    
    # Convert to dense MatrixTable
    mt_chr = hl.vds.to_dense_mt(vds_chr)
    
    # Annotate entries with GT field
    mt_chr = mt_chr.annotate_entries(GT=hl.vds.lgt_to_gt(mt_chr.LGT, mt_chr.LA))
    
    # Perform variant QC
    mt_chr = hl.variant_qc(mt_chr)
    
    # Write the MatrixTable to disk in the specified directory
    output_mt_path = os.path.join(output_dir, f"{chrom}.mt")
    mt_chr.write(output_mt_path, overwrite=True)