import os
import hail as hl
from hail.vds.combiner import VariantDatasetCombiner

# Set resources and file paths
ncpu = "local[20]"
ram = "140g"  # Set as string (in bytes) or adjust as needed
BF = 15    # Branch factor
GBS = 10    # gVCF batch size

# Set Temporary paths

spark_local = '/rds/general/project/lms-ware-analysis/ephemeral/HAIL/spark_local'
tmp_dir = '/rds/general/project/lms-ware-analysis/ephemeral/HAIL/hail_tmp'
log = '/rds/general/project/lms-ware-analysis/ephemeral/HAIL/hail.log'
combiner_tmp = "/rds/general/project/lms-ware-analysis/ephemeral/HAIL/test_combiner_tmp"
save_path = os.path.join(combiner_tmp, "combiner_plan.json")


# Set file paths
gvcf_txt = '/rds/general/project/lms-ware-analysis/live/riyad/Hail/test.txt'
output_vds = "/rds/general/project/lms-ware-analysis/live/riyad/Hail/test.vds"
sample_qc = '/rds/general/project/lms-ware-analysis/live/riyad/Hail/test_sample_qc.ht'


# Initialize Hail
hl.init(
    master = ncpu,
    tmp_dir= tmp_dir,
    spark_conf ={
        'spark.local.dir': spark_local,
        'spark.driver.bindAddress': '0.0.0.0',
        'spark.ui.enabled': 'true',
        'spark.ui.port': '4040',
        'spark.driver.memory': ram,
        'spark.executor.memory': ram,
    },
    log = log
)

hl.default_reference = 'GRCh38'

def get_gvcf_files_from_txt(txt_file):
    """
    Reads a text file with one gVCF file path per line and returns a list of paths.
    """
    with open(txt_file, 'r') as f:
        paths = [line.strip() for line in f if line.strip()]
    return paths

# Read gVCF file paths from the provided text file.
files = get_gvcf_files_from_txt(gvcf_txt)
print("Found {} gVCF files.".format(len(files)))

# Create and run the combiner to merge gVCF files into a VDS.
combiner = hl.vds.new_combiner(
    gvcf_paths=files,
    output_path=output_vds,
    temp_path=combiner_tmp,
    save_path=save_path,
    reference_genome="GRCh38",
    use_genome_default_intervals=True,
    branch_factor=BF,
    gvcf_batch_size=GBS,
    target_records=150_000
    # If merging into an existing VDS, add the vds_paths parameter.
)
combiner.run()

# Load the merged VDS.
vds = hl.vds.read_vds(output_vds)

# Compute sample-level QC and checkpoint the resulting table.
qc_table = hl.vds.sample_qc(
    vds,
    dp_bins=(0, 10, 20, 30, 40, 99),
    gq_bins=(0, 10, 20, 30, 40, 99),
    dp_field='DP'
)
qc_table.write(sample_qc, overwrite=True)
print("Sample QC table written to", sample_qc)

hl.stop()
