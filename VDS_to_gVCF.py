#!/usr/bin/env python
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

# Define VDS path
vds = "pipeline/merge2.vds"

# Define gVCF path
new_gvcf = "5sample.vcf.bgz"


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

# 1. Read your VDS and convert to a dense MatrixTable
vds = hl.vds.read_vds(vds)
dense_mt = hl.vds.to_dense_mt(vds)
# 2. Check which entry fields exist
print("Entry fields:", list(dense_mt.entry))
# 3. Remove (drop) the 'gvcf_info' entry field by selecting all other fields
fields_to_keep = [f for f in dense_mt.entry if f != 'gvcf_info']
dense_mt = dense_mt.select_entries(*fields_to_keep)
# 4. Export to VCF
hl.export_vcf(dense_mt, new_gvcf)