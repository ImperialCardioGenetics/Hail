#!/usr/bin/env python
import os
import hail as hl
from hail.vds.combiner import VariantDatasetCombiner

########################################## INSTRUCTIONS ######################################################
"""
This script is designed to merge gVCF files into either a new or existing VDS, perform sample level QC, partition VDS into MatrixTables by Chr with variant level QC using Hail. If you do not have an existing VDS, comment out the following line of code: ds_paths=[existing_vds_path].
"""



hl.init(
        master="local[cpu]",
        log=log,
        spark_conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.ui.enabled": "false",
            "spark.driver.memory": "ram",
            "spark.executor.memory": "ram"
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
        gvcf_paths=gvcf_files,
        output_path=output_vds,
        temp_path=combiner_tmp,
        save_path=save_path,
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

# Partition VDS in MT by Chr

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
    
    # Write the MatrixTable to disk
    output_mt_path = f"MT/{chrom}.mt"
    mt_chr.write(output_mt_path, overwrite=True)